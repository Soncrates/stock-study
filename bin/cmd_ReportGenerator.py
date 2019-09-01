#!/usr/bin/python

import logging
from reportlab.lib import utils
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT

from libCommon import INI

def prep(*ini_list) :
    ret = {}
    for path, section, key, value_list in INI.loadList(*ini_list) :
        if section not in ret :
           ret[section] = {}
        ret[section][key] = value_list
    return ret

def main(report_ini, ini_list) :
    doc = SimpleDocTemplate("image.pdf", pagesize=letter)
    try :
        return _main(doc, report_ini, ini_list)
    except Exception as e :
        logging.error(e, exc_info=True)

def _main(doc,report_ini, ini_list) :
    report_ini = prep(*report_ini)
    logging.debug(report_ini)
    ret = []
    target = 'summary'
    summary = report_ini.get(target,{})
    target = 'images'
    image_list = summary.get(target,[])
    image_list = map(lambda path : alter_aspect(path, 3.5 * inch), image_list)
    target = 'captions'
    captions_list = summary.get(target,[])
    tbl = addTable(captions_list,image_list)
    ret.append(tbl)
 
    key_list = report_ini.keys()
    portfolio_list = filter(lambda x : 'portfolio' in x, key_list)
    portfolio_list = sorted(portfolio_list)
    for target in portfolio_list :
        summary = report_ini.get(target,{})
        target = 'images'
        image_list = summary.get(target,[])
        image_list[0] = alter_aspect(image_list[0], 4 * inch)
        image_list[1] = alter_aspect(image_list[1], 3.5 * inch)
        target = 'captions'
        captions_list = summary.get(target,[])
        tbl = addTable(captions_list,image_list)
        ret.append(tbl)
    doc.build(ret)

def alter_aspect(path, width) :
    im = utils.ImageReader(path)
    w, h = im.getSize()
    logging.info((w, h))
    aspect = float(width / w)
    logging.info(aspect)
    return Image(path,width, h*aspect)

def addTable(name_list, image_list) :
    styles = getSampleStyleSheet()
    style_right = ParagraphStyle(name='right', parent=styles['Normal'], alignment=TA_RIGHT)
    # here you add your rows and columns, these can be platypus objects
    caption_list = map(lambda caption : Paragraph(caption, styles["Normal"]), name_list)
    ret = [ image_list ,
            caption_list]
    return Table(ret)
    ret = [
       [Paragraph("Hello", styles["Normal"]), Paragraph("World (right)", style_right)],
       [Paragraph("Another", styles["Normal"]), Paragraph("Row (normal)", styles["Normal"])]
    ]
    for i, key in enumerate(name_list) :
        caption = name_list[i]
        im = image_list[i] 
        row = [ im, Paragraph(caption, styles["Normal"]) ]
        ret.append(row)
    return Table(ret)
if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   report_ini = glob('{}/report_generator.ini'.format(local))
   #report_ini = sorted(report_ini)

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   main(report_ini,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))

