import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import aplpy

from astropy.io import fits
from astropy.time import Time

def plot_lightcurve(dflc, obj_id='', days_ago=True):
    filter_color = {1:'green', 2:'red', 3:'pink'}
    if days_ago:
        now = Time.now().jd
        t = dflc.jd - now
        xlabel = 'Days Ago'
    else:
        t = dflc.jd
        xlabel = 'Time (JD)'
    
    plt.figure()
    for fid, color in filter_color.items():
        # plot detections in this filter:
        w = (dflc.fid == fid) & ~dflc.magpsf.isnull()
        if np.sum(w):
            plt.errorbar(t[w],dflc.loc[w,'magpsf'], dflc.loc[w,'sigmapsf'],fmt='.',color=color)
        wnodet = (dflc.fid == fid) & dflc.magpsf.isnull()
        if np.sum(wnodet):
            plt.scatter(t[wnodet],dflc.loc[wnodet,'diffmaglim'], marker='v',color=color,alpha=0.25)
    
    plt.gca().invert_yaxis()
    plt.xlabel(xlabel)
    plt.ylabel('Magnitude')
    if obj_id:
        plt.title(obj_id)

        
def plot_dc_lightcurve(dflc, obj_id='', days_ago=True, ema='', ema_diff='', offset=0, metric_thres=.25, show=True):
    
    filter_color = {1:'green', 2:'red', 3:'pink'}
    if days_ago:
        now = Time.now().jd
        t = dflc.jd - now + offset
        xlabel = 'Days Ago'
    else:
        t = dflc.jd
        xlabel = 'Time (JD)'
    
    fig, ax = plt.subplots()
    for fid, color in filter_color.items():
        # plot detections in this filter:
        w = (dflc.fid == fid) & ~dflc.dc_mag.isnull()
        if np.sum(w):
            ax.errorbar(t[w],dflc.loc[w,'dc_mag'], dflc.loc[w,'dc_sigmag'],fmt='.', markersize=15, color=color)
            if ema:
                ax.plot(t[w],dflc.loc[w,ema],color=color, alpha=.25, label=ema)
            if ema_diff:
                for ii in range(len(w)):
                    if w[ii]:
                        ax.annotate('{:.2f}'.format(-dflc.loc[ii,ema_diff]), (t[ii], dflc.loc[ii, ema]))
                        if dflc.loc[ii,ema_diff] > metric_thres:
                            ax.axvspan(t[ii]-.49, t[ii]+.49, color=color, alpha=0.1, hatch='/')
                        if dflc.loc[ii,ema_diff] < -metric_thres:
                            ax.axvspan(t[ii]-.49, t[ii]+.49, color=color, alpha=0.1)

#         if np.sum(wnodet):
#             plt.scatter(t[wnodet],dflc.loc[wnodet,'dc_mag_ulim'], marker='v',color=color,alpha=0.25)

    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Magnitude')
    if obj_id:
        ax.set_title(obj_id)
    if show:
        plt.show()
    return fig

        
def plot_dc_lightcurve_lim(dflc, obj_id='', days_ago=True, ema='', ema_diff='', offset=0, metric_thres=.25, show=True):
    filter_color = {1:'green', 2:'red', 3:'pink'}
    if days_ago:
        now = Time.now().jd
        t = dflc.jd - now + offset
        xlabel = 'Days Ago'
    else:
        t = dflc.jd
        xlabel = 'Time (JD)'
    
    fig, ax = plt.subplots()
    for fid, color in filter_color.items():
        # plot detections in this filter:
        w = (dflc.fid == fid) & ~dflc.dc_mag.isnull()
        if np.sum(w):
            ax.errorbar(t[w],dflc.loc[w,'dc_mag'], dflc.loc[w,'dc_sigmag'],fmt='.', markersize=15, color=color)
            
            if ema_diff:
                for ii in range(len(w)):
                    if w[ii]:
                        ax.annotate('{:.2f}'.format(-dflc.loc[ii,ema_diff]), (t[ii], dflc.loc[ii, ema]))
                        if dflc.loc[ii,ema_diff] > metric_thres:
                            ax.axvspan(t[ii]-.49, t[ii]+.49, color=color, alpha=0.2, hatch='/')
                        if dflc.loc[ii,ema_diff] < -metric_thres:
                            ax.axvspan(t[ii]-.49, t[ii]+.49, color=color, alpha=0.2)
                        
        wnodet = (dflc.fid == fid) & dflc.dc_mag.isnull()
        if np.sum(wnodet):
            ax.scatter(t[wnodet],dflc.loc[wnodet,'dc_mag_ulim'], marker='v',color=color,alpha=0.25)

        w2 = (dflc.fid == fid) & ~dflc.combined_mag_ulim.isnull()
        if sum(w2): 
            if ema:
                ax.plot(t[w2],dflc.loc[w2,ema],color=color, alpha=.25, label=ema)

            
    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Magnitude')
    if obj_id:
        plt.title(obj_id)
    if show:
        plt.show()
    return fig

def plot_cutout(fits_dir, fig=None, subplot=None, **kwargs):
    with fits.open(fits_dir) as hdul:
        if fig is None:
            fig = plt.figure(figsize=(4,4))
        if subplot is None:
            subplot = (1,1,1)
        ffig = aplpy.FITSFigure(hdul[0],figure=fig, subplot=subplot, **kwargs)
        ffig.show_grayscale(stretch='arcsinh')
    return ffig

def show_stamps(ztf_object_id, im_dir):
    #fig, axes = plt.subplots(1,3, figsize=(12,4))
    
    fig = plt.figure(figsize=(12,4))
    dirs = glob.glob(f"{im_dir}{ztf_object_id}*.fits")

    for i, cutout in enumerate(['Science','Template','Difference']):
        fits_dir = [x for x in dirs if cutout in x][0]
        ffig = plot_cutout(fits_dir, fig=fig, subplot = (1,3,i+1))
        ffig.set_title(cutout)
    fig.show()
    
def show_all(packet):
    fig = plt.figure(figsize=(16,4))
    dflc = make_dataframe(packet)
    plot_lightcurve(dflc,ax = plt.subplot(1,4,1))
    for i, cutout in enumerate(['Science','Template','Difference']):
        stamp = packet['cutout{}'.format(cutout)]['stampData']
        ffig = plot_cutout(stamp, fig=fig, subplot = (1,4,i+2))
        ffig.set_title(cutout)
        
def get_ewm_mag(dflc_obj, tau):
    return dflc_obj['dc_mag'].ewm(halflife=tau, times = dflc_obj['utc']).mean()

def update_value(conn, val_dict, condition, table='ZTF_objects'):
    """Update the value of col with val, for the given conditions
    Ex. val_dict = {'SIMBAD_otype': '\"Sy1\"'}, condition = 'ZTF_object_id = \"ZTF19abkfpqk\"' """
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {table} SET " + ", ".join([f"{col} = {val_dict[col]}" for col in val_dict.keys()]) +
                    f" WHERE {condition}")
    except Error as e:
        raise Exception(f"Error updating values {e}")
    conn.commit()

def mark_seen(conn, ztf_object_id):
    data_to_update = {"seen_flag": 1}
    update_value(conn, data_to_update, f'ZTF_object_id = "{ztf_object_id}"')