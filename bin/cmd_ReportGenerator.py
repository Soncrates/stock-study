#!/usr/bin/python

import logging
from json import loads
from reportlab.lib import utils
from reportlab.lib.enums import TA_RIGHT, TA_CENTER
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image
from reportlab.platypus import ListFlowable, ListItem
from reportlab.platypus import PageBreak
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle as PS

from libCommon import INI

styles = getSampleStyleSheet()
style_caption = PS(name='center', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER)
h1 = PS(name = 'Heading1', fontSize = 14, leading = 16)
h2 = PS(name = 'Heading2', fontSize = 12, leading = 14, leftIndent = 5)
h3 = PS(name = 'Heading3', fontSize = 10, leading = 12, leftIndent = 15)
bullet = PS(name = 'Bullet1', fontSize = 8, leading = 12)

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
        print e

def _main(doc,report_ini, ini_list) :
    report_ini = prep(*report_ini)
    logging.debug(report_ini)

    toc = TableOfContents()
    toc.levelStyles = [h1, h2]

    ret = [ toc, PageBreak(), Paragraph('Portfolio Summary', h1) ]
    target = 'summary'
    summary = report_ini.get(target,{})
    target = 'images'
    image_list = summary.get(target,[])
    image_list = map(lambda path : alter_aspect(path, 3.5 * inch), image_list)
    target = 'captions'
    captions_list = summary.get(target,[])
    tbl = addTable(captions_list,image_list)
    ret.append(tbl)
    ret.append(PageBreak())
 
    key_list = report_ini.keys()
    portfolio_list = filter(lambda x : 'portfolio' in x, key_list)
    portfolio_list = sorted(portfolio_list)
    for target in portfolio_list :
        summary = report_ini.get(target,{})
        local_target = 'name'
        name = summary.get(local_target,target)
        if isinstance(name,list) :
           name = name[0]
        name = name.replace('_',' ')
        ret.append(Paragraph(name, h2))
        local_target = 'images'
        image_list = summary.get(local_target,[])
        image_list[0] = alter_aspect(image_list[0], 4 * inch)
        image_list[1] = alter_aspect(image_list[1], 3.5 * inch)
        local_target = 'captions'
        captions_list = summary.get(local_target,[])
        local_target = 'description'
        target_list = filter(lambda key : 'description' in key, summary)
        target_list = sorted(target_list)
        description_list = map(lambda key : summary.get(key,None), target_list)
        description_list = _modifyDescription(description_list)
        desc = []
        for description in addDescription(description_list) :
            desc.append(description)
        tbl = addTable(captions_list,image_list, desc)
        logging.info(tbl)
        ret.append(tbl)
        ret.append(PageBreak())
    logging.info(ret)
    #doc.build(ret)
    doc.multiBuild(ret)

def alter_aspect(path, width) :
    im = utils.ImageReader(path)
    w, h = im.getSize()
    logging.info((w, h))
    aspect = float(width / w)
    logging.info(aspect)
    return Image(path,width, h*aspect)

def addTable(name_list, image_list,description_list = []) :
    caption_list = map(lambda caption : Paragraph(caption, style_caption), name_list)
    logging.info( description_list )
    if len(description_list) :
    #   description_list =  ListFlowable(description_list, bulletType='bullet')
       description_list = [description_list]
    logging.info( description_list )
    ret = [ image_list , caption_list, description_list]
    # here you add your rows and columns, these can be platypus objects
    return Table(data=ret)

def _modifyDescription(arg_list) :
    if len(arg_list) == 0 :
       return arg_list
    for i, value in enumerate(arg_list) : 
        if '{' != value[0] : continue
        value = value.replace("'",'"')
        value = loads(value)
        arg_list[i] = value
    return arg_list

def addDescription(arg_list) :
    if not isinstance(arg_list,list) :
       return
    for i, value in enumerate(arg_list) :
        if isinstance(value,str) :
           value = Paragraph(value, styles["Normal"])
           yield value
        elif isinstance(value,dict) :
           for header in sorted(value.keys()) :
               logging.info(header)
               content_list = value[header]
               header = Paragraph(header, h3)
               yield header
               for content in addDescriptionContent(content_list) :
                   yield content

def addDescriptionContent(arg_list) :
    if not isinstance(arg_list,list) :
       return
    arg_list = sorted(arg_list, key = lambda i: i['weight']) 
    logging.info(arg_list)
    detail = 'Not Available'
    for i, content in enumerate(arg_list) :
        target = 'weight'
        content[target] = "{}%".format(content[target]).rjust(8,' ')
        content = "{weight} {ticker}".format(**content)
        logging.info(content)
        content = Paragraph(content, bullet)
        yield content

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   report_ini = glob('{}/report_generator.ini'.format(local))
   #report_ini = sorted(report_ini)

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   main(report_ini,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))
