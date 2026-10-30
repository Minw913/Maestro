#!/usr/bin/env python3
"""Render the prebid_v2 workflow preview; no ledger, backend or model access.

The paper's editable TikZ figure is in Overleaf/implementation_1020.tex.
This is a standalone process diagram, not a compiled preview of that document.
"""
from argparse import ArgumentParser
from pathlib import Path


def main():
    parser=ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='Output filename without extension')
    args=parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch,FancyArrowPatch
    fig,ax=plt.subplots(figsize=(16,9));fig.patch.set_facecolor('white')
    ax.set(xlim=(-2,14.8),ylim=(-6.2,3));ax.axis('off')
    blue='#dce8ef';pink='#ffe2e3';ink='#234f78';muted='#596674'
    def box(x,y,w,h,title,body='',face=blue):
        ax.add_patch(FancyBboxPatch((x-w/2,y-h/2),w,h,boxstyle='round,pad=0.05',
            facecolor=face,edgecolor=ink,linewidth=1.2,zorder=2))
        title_y=y+(h/2-.3 if body and h>=2 else .2 if body else 0)
        ax.text(x,title_y,title,ha='center',va='center',fontsize=11,
            fontweight='bold',color='#1d2c39',zorder=3)
        if body:ax.text(x,y-(.2 if h>=2 else .28),body,ha='center',va='center',fontsize=9.3,color=muted,zorder=3)
    def arrow(points,dashed=False,label=None):
        xs,ys=zip(*points)
        ax.plot(xs[:-1],ys[:-1],color=ink,lw=1.2,ls='--' if dashed else '-',zorder=1)
        ax.add_patch(FancyArrowPatch(points[-2],points[-1],arrowstyle='-|>',mutation_scale=14,
            lw=1.2,color=ink,linestyle='--' if dashed else '-',zorder=1))
        if label:
            x,y,text=label;ax.text(x,y,text,ha='center',va='center',fontsize=9,color=muted,
                bbox={'facecolor':'white','edgecolor':'none','pad':2},zorder=4)
    box(0,2,3.0,1,'Warm Start','Balanced free opportunities',pink)
    box(0,-.05,3.0,2.5,'Preparation + Plan',
        'Read code / memory\nRead-only structured reasoning',pink)
    box(4,-.05,3.0,2.5,'Implement refinement',
        'All valid plans, in parallel\nEditGrant + existing limits',pink)
    box(4,-.76,2.6,.53,'Freeze source h0',face=blue)
    box(8,-.05,3,1.25,'Valuation + Bidding','Bid bound to frozen h0')
    box(12,-.05,3,1.25,'Auction / Allocation','Frozen order + provisional hold')
    box(12,-3.7,3,3.5,'Invited candidates only',
        'Fixed sanity precheck\n\nRuntime exception only:\nfresh repair agent, max 3 requests\n\nActual input + receipt + parent source')
    box(8,-3.7,3,1.6,'Formal Evaluation','Ticket binds final hk + lineage\nTraining panel, trusted verifier')
    box(4,-3.7,3,1.6,'Synchronous Settlement','Own-bid debit, reward, history\nBoard + checkpoint')
    arrow([(0,1.5),(0,1.2)])
    arrow([(1.55,-.05),(2.45,-.05)])
    arrow([(5.55,-.05),(6.45,-.05)])
    arrow([(9.55,-.05),(10.45,-.05)])
    arrow([(12,-.73),(12,-1.9)])
    arrow([(10.45,-3.7),(9.55,-3.7)],label=(10,-3.24,'Pass'))
    arrow([(6.45,-3.7),(5.55,-3.7)])
    arrow([(2.45,-3.7),(0,-3.7),(0,-1.35)],label=(0,-2.65,'Publish;\nnext round'))
    arrow([(13.55,-3.7),(14.25,-3.7),(14.25,-.05),(13.55,-.05)],True)
    ax.text(12,-1.3,'Dashed: failed / duplicate → release hold\nnext frozen proposal; no new generation',
        ha='center',va='center',fontsize=9,color=muted)
    ax.text(6.4,-5.65,'One common generation deadline: implementation → bidding → checking / repair / cleanup',
        ha='center',fontsize=11,color=ink,fontweight='bold')
    ax.text(6.4,-6.05,'Plan guides reasoning; code consistency is not an admission gate. '
        'Unfunded candidates have no formal score. Structural repair checks do not prove equivalence.',
        ha='center',fontsize=9,color=muted)
    ax.set_title('MAESTRO: freeze the candidate before bidding',fontsize=20,pad=16,color=ink)
    fig.tight_layout()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    for extension in ('png','svg'):
        fig.savefig(args.output.with_suffix('.'+extension),dpi=180,bbox_inches='tight')
    plt.close(fig)


if __name__=='__main__':main()
