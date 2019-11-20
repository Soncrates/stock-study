#!/usr/bin/env python

import logging
from json import loads
from copy import deepcopy

from reportlab.lib.units import inch
#from reportlab.platypus import Image
from reportlab.platypus import PageBreak
from reportlab.platypus import Paragraph
from reportlab.platypus import Table
from libCommon import INI, CSV, log_exception
from libDebug import trace
from libReport import StockTemplate, ReturnsTemplate, SectorTemplate

class PREP :
    @classmethod
    def prep(cls, *ini_list) :
        ret = {}
        for path, section, key, value_list in INI.loadList(*ini_list) :
            if section not in ret :
               ret[section] = {}
            ret[section][key] = value_list
        return ret

    @classmethod
    def _modifyDescription(cls, arg_list) :
        if len(arg_list) == 0 :
             return arg_list
        for i, value in enumerate(arg_list) : 
            if '{' != value[0] : continue
            value = value.replace("'",'"')
            value = loads(value)
            arg_list[i] = value
        return arg_list

@log_exception
@trace
def main(report_ini, ini_list, csv_list) :
    doc = StockTemplate.initPortrait("image.pdf")

    nasdaq_enrichment = filter(lambda x : 'nasdaq.csv', csv_list)
    if len(nasdaq_enrichment) > 0 :
       nasdaq_enrichment = nasdaq_enrichment[0]
    report_ini = PREP.prep(*report_ini)
    logging.debug(report_ini)

    toc = StockTemplate.initToc()

    ret = [ toc, PageBreak(), Paragraph('Portfolio Summary', StockTemplate.h1) ]
    target = 'summary'
    summary = report_ini.get(target,{})
    target = 'images'
    image_list = summary.get(target,[])
    image_list = map(lambda path : StockTemplate.alter_aspect(path, 3.5 * inch), image_list)
    target = 'captions'
    captions_list = summary.get(target,[])
    tbl = StockTemplate.addTable(captions_list,image_list,[],[])
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
        ret.append(Paragraph(name, StockTemplate.h1))
        local_target = 'images'
        image_list = summary.get(local_target,[])
        image_list[0] = StockTemplate.alter_aspect(image_list[0], 4.5 * inch)
        image_list[1] = StockTemplate.alter_aspect(image_list[1], 3.5 * inch)
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

        tbl = StockTemplate.addTable(captions_list,image_list, diverse_list, returns_list)
        logging.debug(tbl)
        ret.append(tbl)
        ret.append(PageBreak())
    logging.debug(ret)
    #doc.build(ret)
    doc.multiBuild(ret)

class RETURNS :

    @classmethod
    def add(cls, arg_list) :
        row_list = []
        for value in cls._add(arg_list) :
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
        ret.setStyle(ReturnsTemplate.ts)
        return [ret]
    @classmethod
    def _add(cls, arg_list) :
        if not isinstance(arg_list,list) :
           return
        if len(arg_list) < 2 :
           return
        
        general = arg_list[0]
        detail  = arg_list[1]
        key_list = sorted(general.keys())

        yield ReturnsTemplate.initHeaders()

        for key in key_list :
            header = key.replace('_','<br/>')
            row = deepcopy(ReturnsTemplate.defaultRow)
            row.update(general[key])
            row = ReturnsTemplate.transformRow(row)
            summary_row = [header] + row
            summary_row = map( lambda cell : Paragraph(cell, StockTemplate._bulletID), summary_row)
            yield summary_row

            for stock in detail[key].keys() :
                row = deepcopy(ReturnsTemplate.defaultRow)
                row.update(detail[key][stock])
                row = ReturnsTemplate.transformRow(row)
                detail_row = [stock] + row
                detail_row = map(lambda t : Paragraph(t, StockTemplate._bullet), detail_row)
                yield detail_row

class DIVERSE :
    @classmethod
    def add(cls, arg_list, nasdaq_enrichment) :
        ret = []
        for description in cls._add(arg_list,nasdaq_enrichment) :
            ret.append(description)
        return ret
    @classmethod
    def _add(cls, arg_list, nasdaq_enrichment) :
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
                   header = Paragraph(header, StockTemplate.h2)
                   yield header
                   for content in cls._addContent(content_list, nasdaq_enrichment) :
                       yield content

    @classmethod
    def _addContent(cls, arg_list, nasdaq_enrichment) :
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
        column_B = map(lambda x : Paragraph(x,StockTemplate.ticker), column_B)
        column_A = map(lambda x : Paragraph(x,StockTemplate.bullet), column_A)

        row_list = []
        for i, dummy in enumerate(column_A) :
            row = [column_A[i], column_B[i]]
            row_list.append(row)
        widths = [2.9*inch] * 2
        widths[0] = 0.7*inch
        logging.info(widths)
        ret = Table(data=row_list,colWidths=widths)
        ret.setStyle(SectorTemplate.ts)
        logging.debug(ret)
        yield ret

if __name__ == '__main__' :

   import logging
   import sys
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.ERROR)
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.ERROR)


   ini_list = env.list_filenames('local/*.ini')
   report_ini = env.list_filenames('local/report_generator.ini')
   csv_list = env.list_filenames('local/*.csv')

   if len(env.argv) :
      report_ini = env.argv

   main(report_ini,ini_list, csv_list)
