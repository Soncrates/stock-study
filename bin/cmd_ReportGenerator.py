#!/usr/bin/env python

import logging
from json import loads
from copy import deepcopy

from reportlab.lib import utils
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image, TableStyle
from reportlab.platypus import ListFlowable, ListItem
from reportlab.platypus import PageBreak, KeepTogether, Spacer
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle as PS

from libCommon import INI, CSV, log_exception
from libDebug import trace

styles = getSampleStyleSheet()
style_caption = PS(name='caption', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, textColor=colors.gray)
h1 = PS(name = 'Heading1', fontSize = 12, leading = 16, leftIndent = 5 , fontName='Helvetica-Bold')
h2 = PS(name = 'Heading2', fontSize = 10, leading = 14, leftIndent = 10, fontName='Helvetica-Bold')
h3 = PS(name = 'Heading3', fontSize = 10, leading = 12, leftIndent = 15, fontName='Helvetica-Bold')
ticker = PS(name = 'Bullet1', fontSize = 8, leading = 8, alignment=TA_LEFT)
bullet = PS(name = 'Bullet2', fontSize = 8, leading = 8, alignment=TA_RIGHT)
_bullet = PS(name = 'Bullet3', fontSize = 8, leading = 6, alignment=TA_LEFT)
_bulletID = PS(name = 'Bullet4', fontSize = 7, leading = 6, alignment=TA_LEFT, fontName='Helvetica-Bold')

class PREP :
    @staticmethod
    def prep(*ini_list) :
        ret = {}
        for path, section, key, value_list in INI.loadList(*ini_list) :
            if section not in ret :
               ret[section] = {}
            ret[section][key] = value_list
        return ret

    @staticmethod
    def _modifyDescription(arg_list) :
        if len(arg_list) == 0 :
             return arg_list
        for i, value in enumerate(arg_list) : 
            if '{' != value[0] : continue
            value = value.replace("'",'"')
            value = loads(value)
            arg_list[i] = value
        return arg_list

def dep_main(report_ini, ini_list, csv_list) :
    doc = SimpleDocTemplate("image.pdf", pagesize=letter)
    try :
        return _main(doc, report_ini, ini_list, csv_list)
    except Exception as e :
        logging.error(e, exc_info=True)
        print e

@log_exception
@trace
def main(report_ini, ini_list, csv_list) :
    doc = SimpleDocTemplate("image.pdf", pagesize=letter)
    nasdaq_enrichment = filter(lambda x : 'nasdaq.csv', csv_list)
    if len(nasdaq_enrichment) > 0 :
       nasdaq_enrichment = nasdaq_enrichment[0]
    report_ini = PREP.prep(*report_ini)
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
    tbl = MAIN_DOC.addTable(captions_list,image_list,[],[])
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
        name = "Portfolio : {}".format(name)
        ret.append(Paragraph(name, h1))
        local_target = 'images'
        image_list = summary.get(local_target,[])
        image_list[0] = alter_aspect(image_list[0], 4.5 * inch)
        image_list[1] = alter_aspect(image_list[1], 3.5 * inch)
        local_target = 'captions'
        captions_list = summary.get(local_target,[])

        local_target = 'description'
        target_list = filter(lambda key : 'description1' in key, summary)
        target_list = sorted(target_list)
        description_list = map(lambda key : summary.get(key,None), target_list)
        description_list = PREP._modifyDescription(description_list)
        diverse_list = DIVERSE.add(description_list,nasdaq_enrichment) 
        target_list = filter(lambda key : 'description' in key, summary)
        target_list = filter(lambda key : 'description1' not in key, target_list)
        target_list = sorted(target_list)
        description_list = map(lambda key : summary.get(key,None), target_list)
        description_list = PREP._modifyDescription(description_list)
        returns_list = RETURNS.add(description_list)

        tbl = MAIN_DOC.addTable(captions_list,image_list, diverse_list, returns_list)
        logging.debug(tbl)
        ret.append(tbl)
        ret.append(PageBreak())
    logging.debug(ret)
    #doc.build(ret)
    doc.multiBuild(ret)

def alter_aspect(path, width) :
    im = utils.ImageReader(path)
    w, h = im.getSize()
    logging.debug((w, h))
    aspect = float(width / w)
    logging.info(aspect)
    return Image(path,width, h*aspect)

class MAIN_DOC :
    local_ts = [('TOPPADDING', (0,0), (-1,-1), 2),
          ('BOTTOMPADDING', (0,0), (-1,-1), 2),
          ('LEFTPADDING', (0,0), (-1,-1), 1),
          ('RIGHTPADDING', (0,0), (-1,-1), 1),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
          ]
    ts = TableStyle(local_ts)
    @staticmethod
    def addTable(name_list, image_list,diverse_list, returns_list ) :
        caption_list = map(lambda caption : Paragraph(caption, style_caption), name_list)
        logging.debug( diverse_list )
        ret = [ image_list , caption_list, [diverse_list, returns_list]]
        #ret = [ image_list , caption_list, [diverse_list,[Paragraph("Blank?",bullet)]]]
        # here you add your rows and columns, these can be platypus objects
        ret = Table(data=ret)
        ret.setStyle(MAIN_DOC.ts)
        return ret

class RETURNS :
    ts_debug = [('GRID', (0,0), (-1,-1), 0.25, colors.red),
          ('TOPPADDING', (0,0), (-1,-1), 3),
          ('LEFTPADDING', (0,0), (-1,-1), 1),
          ('RIGHTPADDING', (0,0), (-1,-1), 1),
          ('BOTTOMPADDING', (0,0), (-1,-1), 2),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
         ]
    ts = [('TOPPADDING', (0,0), (-1,-1), 2),
          ('BOTTOMPADDING', (0,0), (-1,-1), 2),
          ('LEFTPADDING', (0,0), (-1,-1), 1),
          ('RIGHTPADDING', (0,0), (-1,-1), 1),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
          ]
    ts = TableStyle(ts)
    tableHeaders = [ 'sharpe', 'risk', 'returns', 'weighted sharpe', 'weighted risk', 'weighted returns' ]
    defaultValues = ['-']*len(tableHeaders)
    defaultRow = dict(zip(tableHeaders,defaultValues))

    @staticmethod
    def add(arg_list) :
        row_list = []
        for value in RETURNS._add(arg_list) :
            row_list.append(value)
        #height = [0.1*inch] * len(row_list)
        widths = [0.5*inch] * len(row_list[0])
        widths[0] = 0.9*inch
        widths[1] = 0.45*inch
        widths[2] = 0.32*inch
        widths[3] = 0.45*inch
        #print widths, len(row_list)
        ret = Table(data=row_list,colWidths=widths)
        #debugging tables
        ret.setStyle(RETURNS.ts)
        return [ret]
    @staticmethod
    def _add(arg_list) :
        if not isinstance(arg_list,list) :
           return
        if len(arg_list) < 2 :
           return
        
        general = arg_list[0]
        detail  = arg_list[1]
        key_list = sorted(general.keys())

        header_row = ['Name'] + RETURNS.tableHeaders
        header_row = map(lambda t : Paragraph(t,_bullet),header_row)
        yield header_row

        for key in key_list :
            header = key.replace('_','<br/>')
            row = deepcopy(RETURNS.defaultRow)
            row.update(general[key])
            row = RETURNS._transformRow(row)
            summary_row = [header] + row
            summary_row = map( lambda cell : Paragraph(cell, _bulletID), summary_row)
            yield summary_row

            for stock in detail[key].keys() :
                row = deepcopy(RETURNS.defaultRow)
                row.update(detail[key][stock])
                row = RETURNS._transformRow(row)
                detail_row = [stock] + row
                detail_row = map(lambda t : Paragraph(t, _bullet), detail_row)
                yield detail_row

    @staticmethod
    def _transformRow(row) :
        key_list = filter(lambda key : key in row, RETURNS.tableHeaders)
        data_row = map(lambda x : row[x], key_list)
        data_row = map(lambda cell : RETURNS._transformCell(cell), data_row)
        return data_row

    @staticmethod
    def _transformCell(cell) :
        if isinstance(cell, str) : return cell
        return str(round(cell,2))



class DIVERSE :
    #debugging tables
    ts = [('GRID', (0,0), (-1,-1), 0.25, colors.blue),
          ('TOPPADDING', (0,0), (-1,-1), 1),
          ('BOTTOMPADDING', (0,0), (-1,-1), 1),
          ]
    ts = [('TOPPADDING', (0,0), (-1,-1), 0),
          ('BOTTOMPADDING', (0,0), (-1,-1), 0),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
          ]
    @staticmethod
    def add(arg_list, nasdaq_enrichment) :
        ret = []
        for description in DIVERSE._add(arg_list,nasdaq_enrichment) :
            ret.append(description)
        return ret
    @staticmethod
    def _add(arg_list, nasdaq_enrichment) :
        if not isinstance(arg_list,list) :
           return
        for i, value in enumerate(arg_list) :
            if isinstance(value,str) :
               value = Paragraph(value, styles["Normal"])
               yield value
            elif isinstance(value,dict) :
               for header in sorted(value.keys()) :
                   content_list = value[header]
                   logging.info(header)
                   header = header.replace('_', ' ')
                   header = Paragraph(header, h2)
                   yield header
                   for content in DIVERSE._addContent(content_list, nasdaq_enrichment) :
                       yield content

    @staticmethod
    def _addContent(arg_list, nasdaq_enrichment) :
        if not isinstance(arg_list,list) :
           return
        arg_list = sorted(arg_list, key = lambda i: i['weight']) 
        logging.debug(arg_list)
        column_A, column_B = [], []
        for i, content in enumerate(arg_list) :
            target = 'weight'
            content[target] = "{}%".format(content[target]).rjust(8,' ')
            column_A.append(content['weight'])
            column_B.append(content['ticker'])
        column_C = CSV.grep(nasdaq_enrichment, *column_B)
        for key in column_C :
            description = '({0}) {2}'.format(*column_C[key])[:85]
            description = description.split(' - ')
            description = '<br/>'.join(description)
            column_C[key] = description
        missing_detail = "({0}) No info available for {0}"
        column_B = map(lambda key : column_C.get(key,missing_detail.format(key)),column_B)  
        for column in column_B :
            if 'No info' not in column : 
                logging.info(column)
                continue
            logging.warn(column)
        column_B = map(lambda x : Paragraph(x,ticker), column_B)
        column_A = map(lambda x : Paragraph(x,bullet), column_A)

        row_list = []
        for i, dummy in enumerate(column_A) :
            row = [column_A[i], column_B[i]]
            row_list.append(row)
        widths = [2.9*inch] * 2
        widths[0] = 0.7*inch
        logging.info(widths)
        ret = Table(data=row_list,colWidths=widths)
        ts = TableStyle(DIVERSE.ts)
        ret.setStyle(ts)
        logging.debug(ret)
        yield ret

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   report_ini = env.list_filenames('local/report_generator.ini')
   csv_list = env.list_filenames('local/*.csv')

   main(report_ini,ini_list, csv_list)
