#!/usr/bin/env python

import logging
from json import loads
from copy import deepcopy
import locale

from reportlab.lib.units import inch
from reportlab.platypus import PageBreak
from reportlab.platypus import Paragraph
from reportlab.platypus import Table

from libCommon import INI, CSV, exit_on_exception
from libDebug import trace
from libReport import StockTemplate, ReturnsTemplate, SectorTemplate

class EXTRACT :
    _singleton = None
    @classmethod
    def __init__(self,_env, ini_list,nasdaq_enrichment,input_file,output_file) :
        self.env = _env
        self.ini_list = ini_list
        self.nasdaq_enrichment = nasdaq_enrichment
        self.input_file = input_file
        self.output_file = output_file
        logging.info(vars(self))
    @classmethod
    def instance(cls,**kwargs):
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        _env = globals().get(target,None)
        target = 'ini_list'
        ini_list = globals().get(target,[])
        if not isinstance(ini_list,list) :
           ini_list = [ini_list]
        target = 'nasdaq_enrichment'
        nasdaq_enrichment = globals().get(target,[])
        nasdaq_enrichment = list(nasdaq_enrichment)
        if len(nasdaq_enrichment) > 0 :
           nasdaq_enrichment = nasdaq_enrichment[0]
        target = 'input_file'
        input_file = globals().get(target,None)
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        target = 'output_file'
        output_file = globals().get(target,None)
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        cls._singleton = cls(_env,ini_list,nasdaq_enrichment,input_file,output_file)
        return cls._singleton

    @classmethod
    def config(cls) :
        ini_list = cls.instance().input_file
        logging.info('Reading input file {}'.format(ini_list))
        ret = {}
        for path, section, key, value_list in INI.loadList(*[ini_list]) :
            if section not in ret :
               ret[section] = {}
            ret[section][key] = value_list
        return ret

class TRANSFORM() :
    @classmethod
    def _modifyDescription(cls, arg_list) :
        if not isinstance(arg_list,list) :
           arg_list = list(arg_list)
        if len(arg_list) == 0 :
             return arg_list
        for i, value in enumerate(arg_list) : 
            if '{' != value[0] : continue
            value = value.replace("'",'"')
            value = loads(value)
            arg_list[i] = value
        return arg_list
    @classmethod
    def addSummaryTable(cls, data) :
        locale.setlocale(locale.LC_ALL, '')
        logging.info(data)
        ret = []
        column_headers = ['Initial Balance', 'Final Balance', 'CAGR','Stdev','Sharpe Ratio']
        tbl_header = ['']
        for i, header in enumerate(column_headers) :
            value = Paragraph(header, StockTemplate.bullet)
            tbl_header.append(value)
        ret.append(tbl_header)
        for i, key in enumerate(data):
            rows = [Paragraph(key,StockTemplate.ticker)]
            for j, header in enumerate(column_headers) :
                value = 'Not available'
                if header not in data[key] :
                   logging.warn('NO value for {}'.format(header))
                else :
                   value = data[key][header]
                flag_balance = 'Balance' in header
                flag_percent =  header in ['CAGR','Stdev']
                flag_ratio = header in ['Sharpe Ratio']
                if flag_balance :
                    value = locale.currency(value, grouping=True)
                    value = Paragraph(value,StockTemplate.bullet)
                elif flag_percent :
                    value = str(round(value*100,2))
                    value = value + ' %'
                    value = Paragraph(value,StockTemplate.bullet)
                elif flag_ratio :
                    value = str(round(value,2))
                    value = Paragraph(value,StockTemplate.bullet)
                else :
                    value = Paragraph(value,StockTemplate.bullet)
                rows.append(value)
            ret.append(rows)
        ret = Table(data=ret)
        ret.setStyle(StockTemplate.ts)
        return ret

class DIVERSE :
    @classmethod
    def add(cls, arg_list) :
        ret = []
        for description in cls._add(arg_list) :
            ret.append(description)
        return ret
    @classmethod
    def _add(cls, arg_list) :
        logging.info(type(arg_list))
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
                   for content in cls._addContent(content_list) :
                       yield content

    @classmethod
    def _addContent(cls, arg_list) :
        logging.info(type(arg_list))
        if not isinstance(arg_list,list) :
           return
        _enrichment_file = EXTRACT.instance().nasdaq_enrichment
        logging.info('Reading input file {}'.format(_enrichment_file))
        arg_list = sorted(arg_list, key = lambda i: i['weight']) 
        logging.debug(arg_list)
        column_A, column_B = [], []
        for i, content in enumerate(arg_list) :
            target = 'weight'
            content[target] = "{}%".format(content[target]).rjust(8,' ')
            column_A.append(content['weight'])
            column_B.append(content['ticker'])
        column_B = list(column_B)
        column_C = CSV.grep(_enrichment_file, *column_B)
        for key in column_C :
            description = '({0}) {2}'.format(*column_C[key])[:85]
            description = description.split(' - ')
            description = '<br/>'.join(description)
            column_C[key] = description
        missing_detail = "({0}) No info available for {0}"
        column_B = map(lambda key : column_C.get(key,missing_detail.format(key)),column_B)  
        column_B = list(column_B)
        for column in column_B :
            if 'No info' not in column : 
                logging.info(column)
                continue
            logging.warn(column)
        column_B = map(lambda x : Paragraph(x,StockTemplate.ticker), column_B)
        column_B = list(column_B)
        column_A = map(lambda x : Paragraph(x,StockTemplate.bullet), column_A)
        column_A = list(column_A)
        logging.info(len(column_A))
        logging.info(len(column_B))

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

class RETURNS :

    @classmethod
    def add(cls, arg_list) :
        row_list = []
        for value in cls._add(arg_list) :
            if not isinstance(value,list) :
               value = list(value)
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
            if not isinstance(row,list) :
               row = list(row)
            summary_row = [header] + row
            summary_row = map( lambda cell : Paragraph(cell, StockTemplate._bulletID), summary_row)
            summary_row = list(summary_row)
            yield summary_row

            for stock in detail[key].keys() :
                row = deepcopy(ReturnsTemplate.defaultRow)
                row.update(detail[key][stock])
                row = ReturnsTemplate.transformRow(row)
                if not isinstance(row,list) :
                   row = list(row)
                detail_row = [stock] + row
                detail_row = map(lambda t : Paragraph(t, StockTemplate._bullet), detail_row)
                detail_row = list(detail_row)
                yield detail_row

@exit_on_exception
@trace
def main() :

    output_file = EXTRACT.instance().output_file
    logging.info('Writing report {}'.format(output_file))
    doc = StockTemplate.initPortrait(output_file)
    toc = StockTemplate.initToc()
    ret = [ toc, PageBreak(), Paragraph('Portfolio Summary', StockTemplate.h1) ]

    input_file = EXTRACT.config()
    target = 'summary'
    summary = input_file.get(target,{})
    target = 'images'
    image_list = summary.get(target,[])
    image_list = map(lambda path : StockTemplate.alter_aspect(path, 3.5 * inch), image_list)
    target = 'captions'
    captions_list = summary.get(target,[])
    tbl = StockTemplate.addTable(captions_list,image_list,[],[])
    ret.append(tbl)

    target = 'table'
    summary_text = summary.get(target,{})
    summary_text = summary_text.replace("'",'"')
    summary_text = loads(summary_text)
    summary_tbl = TRANSFORM.addSummaryTable(summary_text) 
    ret.append(summary_tbl)

    ret.append(PageBreak())
 
    key_list = sorted(input_file.keys())
    portfolio_list = filter(lambda x : 'portfolio' in x, key_list)
    for target in portfolio_list :
        summary = input_file.get(target,{})
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
        description_list = TRANSFORM._modifyDescription(description_list)
        diverse_list = DIVERSE.add(description_list) 
        target_list = filter(lambda key : 'description' in key, summary)
        target_list = filter(lambda key : 'description1' not in key, target_list)
        target_list = sorted(target_list)
        description_list = map(lambda key : summary.get(key,None), target_list)
        description_list = TRANSFORM._modifyDescription(description_list)
        returns_list = RETURNS.add(description_list)

        tbl = StockTemplate.addTable(captions_list,image_list, diverse_list, returns_list)
        logging.debug(tbl)
        ret.append(tbl)
        ret.append(PageBreak())

    logging.debug(ret)
    doc.multiBuild(ret)

if __name__ == '__main__' :

   import logging
   import sys
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*.ini')
   csv_list = env.list_filenames('local/*.csv')
   nasdaq_enrichment = filter(lambda x : 'nasdaq.csv', csv_list)

   input_file = env.list_filenames('local/report_generator.ini')
   output_file = "{pwd_parent}/local/image.pdf".format(**vars(env))

   main()
