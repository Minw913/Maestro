"""Matplotlib renderer for read-only search-tree evidence."""
from decimal import Decimal
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyBboxPatch, PathPatch, Rectangle
from matplotlib.path import Path as MPath

RED, INK, GREY = '#c62a36', '#171b22', '#737c89'
PAPER, LINE = '#ffffff', '#dce1e8'
ROW, LANE, LEFT = 2.84, 5.15, 2.2


def money(value):
    return '—' if value is None else f'{Decimal(str(value)):.6f}'


def score(value):
    return f'{value:.9f}'


def txt(ax, x, y, text, **kw):
    ax.text(x, y, text, va='center', color=kw.pop('color', INK), **kw)


def panel(ax, x, y, w, h, face, edge=LINE, lw=.8):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0.025,rounding_size=0.08',
                               facecolor=face, edgecolor=edge, linewidth=lw, zorder=2))


class SearchTreePlot:
    def __init__(self, data, *, dpi=145, detail_rounds=None):
        self.data, self.dpi = data, dpi
        self.agents = data['metadata']['agents']
        self.regime = data['metadata']['regime']
        self.width = max(23.2, LEFT + len(self.agents)*LANE + .4)
        self.market = [r for r in data['rounds'] if r['stage']=='market']
        if detail_rounds:
            try:
                start,end = map(int, detail_rounds.split(':'))
            except (ValueError, AttributeError) as error:
                raise ValueError('--detail-rounds must be START:END') from error
            if not 1 <= start <= end <= len(self.market):
                raise ValueError('--detail-rounds is outside the settled Market range')
        else:
            before = data['initial']['market']['board']['champion_score']
            gains = []
            for row in self.market:
                gains.append(row['champion_score']-before)
                before = row['champion_score']
            center = max(range(len(gains)), key=gains.__getitem__) + 1
            start = max(1, min(center-2, len(self.market)-3))
            end = min(len(self.market), start+3)
        self.detail_range = (start,end)

    def heading(self, ax, title, subtitle, y=0.45):
        txt(ax, .8, y, title, fontsize=25, weight='bold')
        txt(ax, .8, y + .48, subtitle, fontsize=11.4, color=GREY)
        txt(ax, .8, y + .91,
            'Each lane retains its best parent. Funded candidates enter the spine only if they improve it; otherwise they end as side leaves.',
            fontsize=10.3)
        legend_y = y + 1.34
        for x, color, label, dashed in [(.95, RED, 'Global champion after settlement', False),
                                       (7.4, INK, 'Funded / evaluated node', False),
                                       (13.6, INK, 'Not funded: same parent carried', True)]:
            ax.add_patch(Circle((x, legend_y), .10, facecolor='white', edgecolor=color,
                                linewidth=2, linestyle='--' if dashed else '-', zorder=5))
            txt(ax, x + .22, legend_y, label, fontsize=10.1)
        txt(ax, .8, y + 1.75,
            's = best-parent training score   v = pre-auction valuation   b = submitted bid   w = wallet   r = settled reward   q = evaluated candidate score',
            fontsize=10)
        txt(ax, .8, y + 2.1,
            'Arrows in s and w mean before → after this round. v and b belong to this round, not to the next plan. Values are rounded for display.',
            fontsize=9.5, color=GREY)


    def column_headers(self, ax, y):
        directions = [self.data['metadata']['directions'].get(a, '') for a in self.agents]
        for i, direction in enumerate(directions):
            xl = LEFT + i * LANE
            txt(ax, xl + 2.36, y, self.agents[i].upper(), ha='center', fontsize=15, weight='bold')
            txt(ax, xl + 2.36, y+.32, direction, ha='center', fontsize=10, color=GREY)


    def snapshot(self, ax, y, states, champion_hash, label, previous=None):
        txt(ax, .55, y+.43, label, fontsize=11.5, weight='bold')
        anchors = {}
        for i, agent in enumerate(self.agents):
            xl = LEFT + i*LANE
            state = states[agent]
            x, yy = xl+.27, y+.36
            color = RED if state['parent_hash'] == champion_hash else INK
            if previous:
                xp, yp = previous[agent]
                ax.plot([xp,x], [yp,yy], color=GREY, linewidth=1, linestyle=(0,(3,3)), zorder=1)
            panel(ax, xl+.04, y+.13, 4.75, .94, '#f7f8fa')
            ax.add_patch(Circle((x,yy), .105, facecolor='white', edgecolor=color, linewidth=2, zorder=5))
            txt(ax, xl+.58, y+.36, 'P '+state['parent_hash'][:8], fontsize=10, color=color, weight='bold')
            txt(ax, xl+.58, y+.65, 's_i = '+score(state['parent_score']), family='DejaVu Sans Mono', fontsize=9.7)
            if state.get('wealth') is not None:
                txt(ax, xl+.58, y+.91, 'w_i = '+money(state['wealth']), family='DejaVu Sans Mono', fontsize=9.7)
            anchors[agent] = (x,yy)
        return anchors


    def round_row(self, ax, y, row, previous):
        warm = row['stage'] == 'warm'
        ident = ('W' if warm else 'M')+f"{row['round']:02d}"
        txt(ax, .55, y+.32, ident, fontsize=16, weight='bold')
        txt(ax, .55, y+.63, 'FREE' if warm else ('AUCTION' if self.regime in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') else self.regime.upper()), fontsize=8.8, color=GREY)
        holders = [n for n in row['agents'] if n['champion']]
        txt(ax, .55, y+1.33, 'BEST', fontsize=8.7, color=RED, weight='bold')
        txt(ax, .55, y+1.61, ','.join('A'+n['agent'].split('-')[-1] for n in holders), fontsize=13, color=RED, weight='bold')
        txt(ax, .55, y+1.9, f"{row['champion_score']:.6f}", fontsize=9.2, color=RED)
        bar_y = y+.12
        ax.add_patch(Rectangle((LEFT+.04,bar_y), len(self.agents)*LANE-.35,.36,
                               facecolor='#e9edf3', edgecolor=LINE, linewidth=.7, zorder=2))
        new_anchors = {}
        for i, node in enumerate(row['agents']):
            xl = LEFT+i*LANE
            x, yy = xl+.27, y+.91
            funded, accepted = node['funded'], node['accepted']
            color = RED if node['champion'] else INK
            xp, yp = previous[node['agent']]
            # Dashed edges carry the SAME program identity, even if a funded trial lost.
            ax.plot([xp,x],[yp,yy],color=color if accepted else '#9098a3',
                    linewidth=1.6 if accepted else 1.05,
                    linestyle='-' if accepted else (0,(3,3)),zorder=3)
            if funded:
                ax.add_patch(Rectangle((xl+.05,bar_y+.015),4.73,.33,facecolor=INK,zorder=3))
                blabel = 'FREE SLOT' if warm else f"{'WIN' if self.regime in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') else 'SELECTED'}  ·  rank {node['priority_rank'] or '—'}"
                txt(ax,xl+2.41,bar_y+.18,blabel,ha='center',fontsize=9.2,color='white',weight='bold',zorder=4)
            else:
                text = 'NOT FUNDED'
                if node['invitation_status'] == 'FAILED':text = 'TRIAL FAILED → SLOT BACKFILLED'
                txt(ax,xl+2.41,bar_y+.18,text,ha='center',fontsize=8.6,color=GREY,zorder=4)
            panel(ax,xl+.04,y+.72,4.75,1.87,'#fff8f8' if node['champion'] else PAPER,
                  '#e8b9bd' if node['champion'] else LINE)
            ax.add_patch(Circle((x,yy),.11,facecolor='white',edgecolor=color,linewidth=2.1,
                                linestyle='-' if funded else (0,(2.2,1.5)),zorder=6))
            ptag = 'P=C ' if accepted else 'P '
            txt(ax,xl+.58,y+.92,ptag+node['parent_after'][:8],fontsize=10.1,weight='bold',color=color,zorder=6)
            status = 'NEW BEST PARENT' if accepted else ('KEEP BEST PARENT' if funded else 'CARRIED')
            txt(ax,xl+4.60,y+.92,status,fontsize=7.7,ha='right',color=GREY,zorder=6)
            if funded and not accepted:
                # The rejected candidate is a real child of the unchanged parent,
                # not an ancestor of the next round's program.
                ex, ey = xl+4.40, y+1.28
                path = MPath([(x+.13,yy),(xl+.9,y+.49),(xl+4.05,y+.49),(ex,ey-.10)],
                             [MPath.MOVETO,MPath.CURVE4,MPath.CURVE4,MPath.CURVE4])
                ax.add_patch(PathPatch(path,facecolor='none',edgecolor=INK,linewidth=.8,zorder=3))
                ax.add_patch(Circle((ex,ey),.075,facecolor='white',edgecolor=INK,linewidth=1.3,zorder=6))
            txt(ax,xl+.58,y+1.29,f"s_i  {score(node['s_before'])} → {score(node['s_after'])}",
                family='DejaVu Sans Mono',fontsize=9.5,zorder=6)
            if not warm:
                txt(ax,xl+.58,y+1.60,f"v_i  {money(node['valuation'])}    b_i  {money(node['bid'])}",
                    family='DejaVu Sans Mono',fontsize=9.5,zorder=6)
                txt(ax,xl+.58,y+1.91,f"w_i  {money(node['wealth_before'])} → {money(node['wealth_after'])}",
                    family='DejaVu Sans Mono',fontsize=9.5,zorder=6)
            if funded:
                txt(ax,xl+.58,y+2.21,(f"score credit  {money(node['reward'])}" if warm else f"r_i  {money(node['reward'])}    pay  {money(node['payment'])}"),
                    family='DejaVu Sans Mono',fontsize=9.5,zorder=6)
                qlabel = f"C {node['candidate_hash'][:8]}  q={score(node['candidate_score'])}"
                if not accepted:qlabel += '  [leaf]'
                if node['candidate_status'] != 'COMPLETED':
                    qlabel += '  '+('INVALID' if not node['candidate_valid'] else 'TIMEOUTS')
                txt(ax,xl+.58,y+2.48,qlabel,fontsize=8.1,color=GREY,zorder=6)
            else:
                label=(f"Frozen {node['proposal_hash'][:8]}; no formal score"
                       if node.get('proposal_hash') else 'No formal candidate; no payment; no reward')
                txt(ax,xl+.58,y+2.22,label,fontsize=9.1,color=GREY,zorder=6)
                if node['invitation_status']=='FAILED':
                    txt(ax,xl+.58,y+2.48,'Generation/precheck trial failed before admission',fontsize=8.1,color=GREY,zorder=6)
            new_anchors[node['agent']] = (x,yy)
        return new_anchors


    def make_canvas(self, height):
        fig = plt.figure(figsize=(self.width, height), facecolor=PAPER)
        ax = fig.add_axes([0,0,1,1])
        ax.set_xlim(0,self.width);ax.set_ylim(height,0);ax.axis('off')
        return fig,ax


    def footer(self, ax,y,text):
        ax.plot([.8,self.width-.65],[y,y],color=LINE,linewidth=.8)
        txt(ax,.8,y+.35,text,fontsize=10)
        txt(ax,.8,y+.71,'Source: frozen round snapshots, submissions, final allocation tickets, verified results, settlements and checkpoints. No new model/evaluation calls.',fontsize=9,color=GREY)
        txt(ax,.8,y+1.05,f"{self.data['metadata']['benchmark']} · {self.data['metadata']['instance_count']} training instances · {self.data['metadata']['model']} · seed {self.data['metadata']['seed']} · 1-based rounds · exact values: nodes.json / nodes.csv",fontsize=9,color=GREY)



    def render(self, out):
        # Keep plotting style local to this report command.
        with plt.rc_context({'font.family':'DejaVu Sans','font.size':10,
                             'svg.fonttype':'none','axes.unicode_minus':False}):
            return self._render(out)

    def _render(self, out):
        data = self.data
        warm = [r for r in data['rounds'] if r['stage']=='warm']
        partial = ' · PARTIAL: SETTLED ROUNDS ONLY' if data['metadata']['status']!='COMPLETED' else ''
        regime = self.regime.replace('_','-').upper()
        if self.regime=='ucb':
            regime+=f" (exploration={data['metadata'].get('ucb_exploration_coefficient',1.0):g})"
        height = 5.70 + ROW*len(data['rounds']) + (2.10 if warm else 0) + 1.70
        fig,ax = self.make_canvas(height)
        self.heading(ax, regime+' / SEARCH TREE'+partial,
                     f"{data['metadata']['benchmark']} · {data['metadata']['model']} · {len(warm)} Warm waves → {len(self.market)} Market rounds · recorded ledger evidence")
        self.column_headers(ax,3.36)
        if warm:
            txt(ax,.8,4.05,'WARM START · Free allocations; no wallet or bidding.',fontsize=11,weight='bold')
            state = data['initial']['warm']
        else:
            txt(ax,.8,4.05,'MARKET · No linked Warm episode is stored in this ledger.',fontsize=11,weight='bold')
            state = data['initial']['market']
        anchors = self.snapshot(ax,4.35,state['states'],state['board']['champion_hash'],'W00' if warm else 'M00')
        y = 5.70
        for row in warm:
            anchors = self.round_row(ax,y,row,anchors); y += ROW
        if warm:
            boundary = data['metadata']['boundary']
            values = list(boundary['market_initial_wealth'].values())
            if len(set(map(Decimal,values)))==1:
                wallet_note = 'Market wallets start at '+money(values[0])+'.'
            else:
                wallet_note = 'Market initial wallets: '+', '.join(a+'='+money(w) for a,w in boundary['market_initial_wealth'].items())
            panel(ax,.6,y+.03,self.width-1.2,.64,'#eef2f7')
            txt(ax,.85,y+.34,'MARKET BOUNDARY · Same parent programs and histories. '+wallet_note,fontsize=10.1,weight='bold')
            state = data['initial']['market']
            anchors = self.snapshot(ax,y+.77,state['states'],state['board']['champion_hash'],'M00',anchors)
            y += 2.10
        for row in self.market:
            anchors = self.round_row(ax,y,row,anchors); y += ROW
        last = self.market[-1]
        owners = ', '.join(n['agent'] for n in last['agents'] if n['champion'])
        payment_note = 'Admitted first-price payment = own bid.' if self.regime in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') else 'Allocation uses '+self.regime+' selection; auction payment = 0.'
        self.footer(ax,y+.07,f"Last recorded champion: {owners} · training score {last['champion_score']:.9f}. "+payment_note)
        full = out/'search-tree.png'; svg = out/'search-tree.svg'
        try:
            fig.savefig(full,dpi=self.dpi,facecolor=PAPER)
            fig.savefig(svg,facecolor=PAPER)
        finally:
            plt.close(fig)
        start,end = self.detail_range
        chosen = [r for r in self.market if start<=r['round']<=end]
        fig,ax = self.make_canvas(5.0+ROW*len(chosen)+1.6)
        self.heading(ax,f'{regime} / MARKET ROUNDS {start}–{end}'+partial,
                     f"{data['metadata']['benchmark']} · {data['metadata']['model']} · same evidence and encoding as the full tree")
        self.column_headers(ax,3.24)
        if start==1:
            before = data['initial']['market']; states = before['states']; champion = before['board']['champion_hash']
        else:
            before = self.market[start-2]
            states = {n['agent']:{'parent_hash':n['parent_after'],'parent_score':n['s_after'],'wealth':n['wealth_after']} for n in before['agents']}
            champion = before['champion_hash']
        anchors = self.snapshot(ax,3.78,states,champion,f'M{start-1:02d}')
        y=5.0
        for row in chosen:
            anchors = self.round_row(ax,y,row,anchors);y+=ROW
        self.footer(ax,y+.02,'Dashed spine = same parent. Dashed circle = not funded. Small black leaf = evaluated candidate not adopted. Missing valuation/bid is shown as —, not zero.')
        detail = out/'search-tree-detail.png'; detail_svg = out/'search-tree-detail.svg'
        try:
            fig.savefig(detail,dpi=self.dpi,facecolor=PAPER)
            fig.savefig(detail_svg,facecolor=PAPER)
        finally:
            plt.close(fig)
        return [full,svg,detail,detail_svg]
