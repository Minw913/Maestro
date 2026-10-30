"""Conservative structural checks, not a proof of semantic equivalence.

Permit large definition moves/restores while rejecting observed call deletion
and no-op function substitutions. A rejected repair abandons the candidate;
these checks never authorize an extra model request or performance search.
"""
import ast
from collections import Counter


def _tree(text):
    try:return ast.parse(text)
    except SyntaxError:return None  # Non-Python launchers are allowed.


def _calls(tree):
    return Counter(ast.dump(n.func,include_attributes=False) for n in ast.walk(tree)
                   if isinstance(n,ast.Call))


def source_index(files):
    """Exact range hints for structural edits; never execute candidate source."""
    result=[]
    for file in files:
        tree=_tree(file['content'])
        result.append({'path':file['path'],'line_count':len(file['content'].splitlines()),
            'definitions':[{'name':node.name,'start_line':node.lineno,'end_line':node.end_lineno,
                            'indent':node.col_offset}
                for node in ast.walk(tree) if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
                if tree is not None else []})
    return result


def check_structural_repair(before,after,parent_files=()):
    parents={f['path']:f['content'] for f in parent_files}
    for path,old_text in before.items():
        new_text=after.get(path,old_text)
        if new_text==old_text:continue
        old=_tree(old_text)
        if old is None:
            raise ValueError('Runtime repair cannot structurally verify changes to a non-Python launcher')
        new=_tree(new_text)
        if new is None:raise ValueError('Runtime repair introduced invalid Python syntax')
        if _calls(old)-_calls(new):
            raise ValueError('Runtime repair removes an existing call; function-disabling fixes are rejected')
        functions=(ast.FunctionDef,ast.AsyncFunctionDef)
        old_defs=Counter(n.name for n in ast.walk(old) if isinstance(n,functions))
        new_defs=Counter(n.name for n in ast.walk(new) if isinstance(n,functions))
        if old_defs-new_defs:raise ValueError('Runtime repair removes an existing function')
        # Lambda stubs cannot stand in for missing runtime functionality.
        old_lambdas=Counter(ast.dump(n,include_attributes=False) for n in ast.walk(old) if isinstance(n,ast.Lambda))
        new_lambdas=Counter(ast.dump(n,include_attributes=False) for n in ast.walk(new) if isinstance(n,ast.Lambda))
        if new_lambdas-old_lambdas:raise ValueError('Runtime repair introduces a lambda replacement')
        parent=_tree(parents.get(path,''))
        if parent is not None:
            definitions={n.name:ast.dump(n,include_attributes=False) for n in ast.walk(parent) if isinstance(n,functions)}
            for node in ast.walk(new):
                if isinstance(node,functions) and node.name not in old_defs and node.name in definitions:
                    if ast.dump(node,include_attributes=False)!=definitions[node.name]:
                        raise ValueError('Missing parent function must be restored intact, not replaced by a stub')
        # Reject newly inserted exception suppression (try/except ...: pass).
        def suppressions(tree):
            return Counter(ast.dump(n,include_attributes=False) for n in ast.walk(tree)
                if isinstance(n,ast.ExceptHandler) and all(isinstance(s,(ast.Pass,ast.Return)) for s in n.body))
        if suppressions(new)-suppressions(old):
            raise ValueError('Runtime repair introduces exception suppression')
