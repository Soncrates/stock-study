#!/usr/bin/python

import logging
from reportlab.lib import utils
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT


def main(file_list, ini_list) :
    doc = SimpleDocTemplate("image.pdf", pagesize=letter)
    try :
        return _main(doc, file_list, ini_list)
    except Exception as e :
        logging.error(e, exc_info=True)

def _main(doc,file_list, ini_list) :
    returns_list, diverse_list, other_list = partition_portfolios(file_list, ini_list)
    ret = []
    image_list = map(lambda x : file_list[x], other_list)
    image_list = map(lambda path : alter_aspect(path, 3.5 * inch), image_list)
    tbl = addTable(other_list,image_list)
    ret.append(tbl)
    image_div_list = map(lambda x : file_list[x], diverse_list)
    image_div_list = map(lambda path : alter_aspect(path, 4 * inch), image_div_list)
    image_return_list = map(lambda x : file_list[x], returns_list)
    image_return_list = map(lambda path : alter_aspect(path, 3.5 * inch), image_return_list)

    name_list = []
    image_list = []
    for i, key in enumerate(returns_list) :
        name_list.append(diverse_list[i])
        name_list.append(returns_list[i])
        image_list.append(image_div_list[i])
        image_list.append(image_return_list[i])
        tbl = addTable(name_list,image_list)
        ret.append(tbl)
        name_list = []
        image_list = []
    doc.build(ret)

def partition_portfolios(file_list, ini_list) :
    logging.debug(file_list)
    key_list = sorted(file_list.keys())
    returns_list = filter(lambda x : 'returns' in x, key_list)
    logging.info(returns_list)
    diverse_list = filter(lambda x : 'diversified' in x, key_list)
    logging.info(diverse_list)
    other_list = list(set(key_list) - set(returns_list) - set(diverse_list))
    other_list = sorted(other_list)
    logging.info(other_list)
    return returns_list, diverse_list, other_list

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

   pwd = os.getcwd()
   sys.path.append(sys.path[0].replace('test','bin'))
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   local = pwd.replace('test','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/portfolio*.png'.format(local))
   #file_list = sorted(file_list)
   file_name = map(lambda x : os.path.basename(x), file_list)
   file_list = dict(zip(file_name, file_list))

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   main(file_list,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))

