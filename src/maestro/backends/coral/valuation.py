"""Fresh native Coral sessions with only a dedicated valuation read view."""
import json
from pathlib import Path
import time
from maestro.agents.valuation_reasoning import VALUATION_PROMPT,SCHEMA,ValuationReadBroker
from maestro.backends.base import BackendUnavailable
from maestro.llm import InferenceRequestUnknown


class ValuationOutputLimit(ValueError):
    """A terminal, locally reclaimed valuation exhausted its final output cap."""


class ValuationRepairFailed(ValueError):
    """The one authorized final-answer regeneration failed with settled usage."""


def _repair_prompt(files,broker):
    """Give a fresh, tool-free session the evidence already exposed to the agent."""
    paths=broker.evidence_paths()
    if not {'public/contract.json','public/trajectory.json'}<=set(paths) or not any(
            path.startswith('solver/') for path in paths):
        raise ValuationRepairFailed('Required valuation evidence was not read before truncation')
    blocks=[];remaining=220*1024
    for path in paths:
        contents=files[path]
        # The original virtual files are screened; keep the same evidence set.
        block=json.dumps({'path':path,'content':contents},ensure_ascii=False)
        size=len(block.encode())
        if size>remaining:
            raise ValuationRepairFailed('Read evidence exceeds bounded one-request regeneration context')
        blocks.append(block);remaining-=size
    return ('VALUATION_RO: The final structured answer was incomplete or malformed. This is one '
        'tool-free regeneration request within the same deadline. Use only the '
        'following previously read evidence. Return one complete, concise JSON '
        'object matching the schema. Do not emit DSML, tool markup, a tool call, '
        'or another turn.\n'
        + '\n'.join(blocks))


class ValuationBackendMixin:
    valuation_capabilities={'read_only':True,'actual_request_gate':True,'fresh_session':True,'no_retry':True}

    def _valuation_proxy(self,account,deadline):
        return self._proxy(account,deadline,self.config.valuation_calls,tools=True)

    def _terminal_output_limit(self,account,agent_id,proxy,error,cleanup):
        """Identify a capped incomplete answer without excusing an unknown request."""
        if not isinstance(error,BackendUnavailable) or proxy is None:
            return False
        usage=self.budget_store.usage(account)
        if (usage.get('calls')!=self.config.valuation_calls or
                usage.get('usage_complete') is not True or
                usage.get('unresolved_requests') or usage.get('unknown_requests') or
                not cleanup.get('local_process_group_reclaimed') or
                not cleanup.get('tool_service_reclaimed')):
            return False
        with self.budget_store._lock:
            row=self.budget_store.connection.execute('''
                SELECT r.status,r.output_tokens AS ceiling,p.output_tokens AS actual,
                       p.usage_complete FROM inference_reservations r
                LEFT JOIN provider_receipts p USING(reservation_id)
                WHERE r.account=? ORDER BY r.rowid DESC LIMIT 1''',(account,)).fetchone()
        if (row is None or row['status']!='COMPLETED' or row['usage_complete']!=1
                or row['actual']!=row['ceiling']
                or row['ceiling']!=getattr(proxy,'final_request_tokens',
                                          getattr(proxy,'per_request_tokens',None))):
            return False
        phase=cleanup.get('phase_path')
        if not phase:return False
        log=Path(phase)/'public'/'logs'/f'{agent_id}.0.log'
        try:
            lines=log.read_text(encoding='utf-8').splitlines()
            if not lines:return False
            header=json.loads(lines[0])
            if (header.get('type')!='coral' or header.get('agent_id')!=agent_id
                    or not header.get('prompt','').startswith('VALUATION_RO:')):
                return False
        except (OSError,UnicodeError,ValueError,TypeError,AttributeError):
            return False
        answer='\n'.join(lines[1:]).strip()
        if not answer:return True
        try:json.loads(answer)
        except ValueError:return True
        return False

    def _terminal_malformed_json(self,account,agent_id,proxy,error,cleanup):
        """A complete fourth receipt and reclaimed native process emitted non-JSON."""
        if not isinstance(error,json.JSONDecodeError) or proxy is None:
            return False
        usage=self.budget_store.usage(account)
        if (usage.get('calls')!=self.config.valuation_calls or usage.get('usage_complete') is not True
                or usage.get('unresolved_requests') or usage.get('unknown_requests')
                or not cleanup.get('local_process_group_reclaimed')
                or not cleanup.get('tool_service_reclaimed')):
            return False
        with self.budget_store._lock:
            row=self.budget_store.connection.execute('''SELECT r.status,p.usage_complete
                FROM inference_reservations r LEFT JOIN provider_receipts p USING(reservation_id)
                WHERE r.account=? ORDER BY r.rowid DESC LIMIT 1''',(account,)).fetchone()
        if row is None or row['status']!='COMPLETED' or row['usage_complete']!=1:
            return False
        phase=cleanup.get('phase_path')
        if not phase:return False
        try:
            lines=(Path(phase)/'public'/'logs'/f'{agent_id}.0.log').read_text(encoding='utf-8').splitlines()
            header=json.loads(lines[0])
            if (header.get('type')!='coral' or header.get('agent_id')!=agent_id
                    or not header.get('prompt','').startswith('VALUATION_RO:')):
                return False
            json.loads('\n'.join(lines[1:]).strip())
        except json.JSONDecodeError:
            return True
        except (OSError,UnicodeError,ValueError,TypeError,AttributeError,IndexError):
            return False
        return False

    def value_with_receipt(self,agent_id,round_index,files,*,deadline):
        account=f'{getattr(self,"request_episode_id",self.episode_id)}:{round_index}:{agent_id}:VALUATION_RO'
        with self.budget_store._lock:
            if self.budget_store.connection.execute('SELECT 1 FROM codex_phase_scopes WHERE account=?',(account,)).fetchone():
                raise PermissionError('Valuation scope already consumed')
        if time.monotonic()>=deadline:raise TimeoutError('Expired valuation scope')
        broker=ValuationReadBroker(files,deadline,self._dispatch_guard)
        proxy=None
        def tool(name,args):
            try:value=broker(name,args)
            except (PermissionError,ValueError):return {'error':'Read request denied; use listed virtual paths and read operations only'}
            used=self.budget_store.usage(account)['calls']
            return {**value,'request_budget':{'actual_requests':used,'remaining_requests':max(0,self.config.valuation_calls-used)}}
        prompt=VALUATION_PROMPT.replace('REQUEST_CAP',str(self.config.valuation_calls))
        repair_attempted=False
        try:
            with self._phase(account):
                proxy=self._valuation_proxy(account,deadline)
                try:
                    bind_evidence=getattr(proxy,'bind_final_evidence_paths',None)
                    if callable(bind_evidence):
                        bind_evidence(broker.evidence_paths)
                    identity=({'agent_id':agent_id} if getattr(self.client,'supports_phase_identity',False) else {})
                    try:
                        response=self.client.run_phase(prompt,base_url=proxy.start(),deadline=deadline,
                            tools=tool,output_schema=SCHEMA,session_id=None,**identity)
                    except Exception as error:
                        if self.budget_store.usage(account).get('unresolved_requests',0) or getattr(error,'inflight_unknown',False):
                            raise
                        cleanup=self.client.cleanup_receipt(getattr(proxy,'url',None)) if hasattr(self.client,'cleanup_receipt') else {}
                        capped=self._terminal_output_limit(account,agent_id,proxy,error,cleanup)
                        malformed=(not capped and self._terminal_malformed_json(
                            account,agent_id,proxy,error,cleanup))
                        if capped or malformed:
                            enable=getattr(proxy,'enable_final_retry',None)
                            if not callable(enable):
                                if capped:raise ValuationOutputLimit('Final valuation answer exhausted its output-token limit') from error
                                raise
                            repair_attempted=True
                            try:
                                repair_prompt=_repair_prompt(files,broker)
                                enable('output_cap' if capped else 'malformed_json')
                                if time.monotonic()>=deadline:raise TimeoutError('Valuation repair deadline expired')
                                response=self.client.run_phase(repair_prompt,base_url=proxy.url,deadline=deadline,
                                    tools=None,output_schema=SCHEMA,session_id=None,**identity)
                            except Exception as repair_error:
                                repair_usage=self.budget_store.usage(account)
                                if (repair_usage.get('unresolved_requests') or repair_usage.get('unknown_requests')
                                        or repair_usage.get('usage_complete') is not True
                                        or getattr(repair_error,'inflight_unknown',False)):
                                    raise InferenceRequestUnknown('Valuation repair requires reconciliation',
                                        request_id=account,scope_id=account,context={}) from repair_error
                                repair_cleanup=self.client.cleanup_receipt(getattr(proxy,'url',None)) if hasattr(self.client,'cleanup_receipt') else {}
                                if not self.simulation and not (repair_cleanup.get('local_process_group_reclaimed')
                                        and repair_cleanup.get('tool_service_reclaimed')):
                                    raise InferenceRequestUnknown('Valuation repair cleanup unproven',
                                        request_id=account,scope_id=account,context={}) from repair_error
                                raise ValuationRepairFailed('One final-answer regeneration failed') from repair_error
                        else:
                            raise
                    proxy.assert_resolved()
                    usage=proxy.usage()
                    cleanup=self.client.cleanup_receipt(getattr(proxy,'url',None)) if hasattr(self.client,'cleanup_receipt') else {}
                    if not self.simulation and not (cleanup.get('local_process_group_reclaimed') and cleanup.get('tool_service_reclaimed')):
                        raise InferenceRequestUnknown('Valuation cleanup proof missing',request_id=account,scope_id=account,context={})
                    if usage.get('usage_complete') is not True:
                        raise InferenceRequestUnknown('Valuation usage incomplete',request_id=account,scope_id=account,context={})
                    if not 1<=usage['calls']<=self.config.valuation_calls+int(repair_attempted) or usage.get('protocol_violations') or usage.get('terminal_failures'):
                        raise ValueError('Valuation request contract violated')
                    if time.monotonic()>=deadline:raise TimeoutError('Late valuation result')
                    return {'text':response.text,'usage':usage,'read_trace':broker.trace,'cleanup':cleanup,
                        'repair_attempted':repair_attempted}
                except Exception as error:
                    if self.budget_store.usage(account).get('unresolved_requests',0) or getattr(error,'inflight_unknown',False):
                        raise InferenceRequestUnknown('Valuation requires reconciliation',request_id=account,scope_id=account,context={}) from error
                    cleanup={}
                    if not self.simulation and proxy is not None:
                        cleanup=self.client.cleanup_receipt(getattr(proxy,'url',None)) if hasattr(self.client,'cleanup_receipt') else {}
                        if not (cleanup.get('local_process_group_reclaimed') and cleanup.get('tool_service_reclaimed')):
                            raise InferenceRequestUnknown('Valuation failure cleanup unproven',request_id=account,
                                scope_id=account,context={}) from error
                    if self._terminal_output_limit(account,agent_id,proxy,error,cleanup):
                        raise ValuationOutputLimit('Final valuation answer exhausted its output-token limit') from error
                    if repair_attempted and isinstance(error,(ValueError,TimeoutError,BackendUnavailable)):
                        raise ValuationRepairFailed('One final-answer regeneration failed') from error
                    raise
                finally:
                    if proxy is not None:proxy.close()
        finally:broker.close()
