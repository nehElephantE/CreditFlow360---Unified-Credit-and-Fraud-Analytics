import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ChartUtils:
    
    @staticmethod
    def set_style():
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (12, 6)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
    
    @staticmethod
    def format_currency(ax):
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'₹{x:,.0f}'))
    
    @staticmethod
    def format_percentage(ax):
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
    
    @staticmethod
    def save_chart(fig, filename, subdir=''):
        save_dir = Path(f'analytics/reports/{subdir}')
        save_dir.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_dir / filename, bbox_inches='tight', dpi=300)
        plt.close(fig)
        logger.info(f"✅ Chart saved: {save_dir / filename}")
    
    @staticmethod
    def create_heatmap(data, title, xlabel, ylabel, annot=True, cmap='RdYlGn_r'):
        fig, ax = plt.subplots(figsize=(14, 8))
        sns.heatmap(data, annot=annot, fmt='.1f', cmap=cmap, 
                   linewidths=0.5, ax=ax, cbar_kws={'label': 'Value'})
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        return fig
    
    @staticmethod
    def create_bar_chart(df, x, y, title, xlabel, ylabel, color_by=None, 
                        horizontal=False, sort=True):
        fig, ax = plt.subplots(figsize=(12, 6))
        
        if sort:
            df = df.sort_values(y, ascending=False)
        
        if horizontal:
            if color_by:
                bars = ax.barh(df[x], df[y], color=plt.cm.viridis(df[color_by]))
            else:
                bars = ax.barh(df[x], df[y], color='steelblue')
            ax.set_xlabel(ylabel)
            ax.set_ylabel(xlabel)
        else:
            if color_by:
                bars = ax.bar(df[x], df[y], color=plt.cm.viridis(df[color_by]))
            else:
                bars = ax.bar(df[x], df[y], color='steelblue')
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            plt.xticks(rotation=45, ha='right')
        
        ax.set_title(title, fontsize=16, fontweight='bold')
        
        for bar in bars:
            height = bar.get_height() if not horizontal else bar.get_width()
            if horizontal:
                ax.text(height + max(df[y])*0.01, bar.get_y() + bar.get_height()/2,
                       f'{height:,.0f}', va='center', fontsize=9)
            else:
                ax.text(bar.get_x() + bar.get_width()/2, height + max(df[y])*0.01,
                       f'{height:,.0f}', ha='center', fontsize=9)
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def create_line_chart(df, x, y, title, xlabel, ylabel, hue=None, 
                          markers=True, ci=None):
        fig, ax = plt.subplots(figsize=(14, 6))
        
        if hue:
            for name, group in df.groupby(hue):
                ax.plot(group[x], group[y], marker='o' if markers else None, 
                       linewidth=2, label=name)
            ax.legend(title=hue)
        else:
            ax.plot(df[x], df[y], marker='o' if markers else None, 
                   linewidth=2, color='steelblue')
        
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        return fig
    
    @staticmethod
    def create_pie_chart(data, labels, title, autopct='%1.1f%%', 
                        startangle=90, shadow=True):
        fig, ax = plt.subplots(figsize=(10, 8))
        colors = plt.cm.Set3(np.linspace(0, 1, len(data)))
        
        wedges, texts, autotexts = ax.pie(data, labels=labels, autopct=autopct,
                                          startangle=startangle, shadow=shadow,
                                          colors=colors, textprops={'fontsize': 12})
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.axis('equal')
        plt.tight_layout()
        return fig