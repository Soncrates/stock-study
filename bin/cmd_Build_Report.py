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
    def __init__(self,_env, ini_list,input_file,output_file) :
        self.env = _env
        self.ini_list = ini_list
        self.input_file = input_file
        self.output_file = output_file
        msg = vars(self)
        for i, key in enumerate(sorted(msg)) :
            value = msg[key]
            if isinstance(value,list) and len(value) > 10 :
               value = value[:10]
            logging.info((i,key, value))

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
        target = 'input_file'
        input_file = globals().get(target,None)
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        target = 'output_file'
        output_file = globals().get(target,None)
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        cls._singleton = cls(_env,ini_list,input_file,output_file)
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
    column_headers = ['Initial Balance', 'Final Balance', 'CAGR','Stdev','Sharpe Ratio']
    @classmethod
    def _modifyDescription(cls, arg_list) :
        if not isinstance(arg_list,list) :
           arg_list = list(arg_list)
        if len(arg_list) == 0 :
             return arg_list
        for i, value in enumerate(arg_list) : 
            if '{' != value[0] : continue
            value = value.replace("'",'"')
            logging.info(value)
            value = loads(value)
            arg_list[i] = value
        return arg_list
    @classmethod
    def humanReadable(cls, header, ret) :
        flag_currency = header in ['Initial Balance', 'Final Balance']
        flag_percent =  header in ['CAGR','Stdev']
        flag_ratio = header in ['Sharpe Ratio']
        if flag_currency :
           ret = locale.currency(ret, grouping=True)
           ret = Paragraph(ret,StockTemplate.bullet)
        elif flag_percent :
           logging.info(ret)
           if ret == 'Unkown' : 
              ret = 0.0
           ret = str(round(ret*100,2))
           ret = ret + ' %'
           ret = Paragraph(ret,StockTemplate.bullet)
        elif flag_ratio :
           if ret == 'Unkown' : 
              ret = 0.0
           ret = str(round(ret,2))
           ret = Paragraph(ret,StockTemplate.bullet)
        else :
           ret = Paragraph(ret,StockTemplate.bullet)
        return ret
    @classmethod
    def _addSummaryTable(cls, key, data) :
        rows = [Paragraph(key,StockTemplate.ticker)]
        for j, header in enumerate(cls.column_headers) :
            value = 'Not available'
            if header not in data[key] :
                   logging.warn('No value for {}'.format(header))
            else :
               value = data[key][header]
            value = cls.humanReadable(header,value)
            rows.append(value)
        return rows
    @classmethod
    def addSummaryTable(cls, data) :
        locale.setlocale(locale.LC_ALL, '')
        logging.info(data)
        ret = []
        tbl_header = ['']
        for i, header in enumerate(cls.column_headers) :
            value = Paragraph(header, StockTemplate.bullet)
            tbl_header.append(value)
        ret.append(tbl_header)
        for i, key in enumerate(data):
            rows = cls._addSummaryTable(key,data)
            ret.append(rows)
        ret = Table(data=ret)
        ret.setStyle(StockTemplate.ts)
        return ret
    @classmethod
    def percent(cls, data) :
        ret = "{}%".format(data).rjust(8,' ')
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
        arg_list = sorted(arg_list, key = lambda i: i['weight']) 
        logging.debug(arg_list)
        weight_list, ticker_list, name_list = [], [], []
        for i, content in enumerate(arg_list) :
            ticker_list.append(content['ticker'])
            target = 'weight'
            content[target] = TRANSFORM.percent(content[target])
            weight_list.append(content[target])
            target = 'Name'
            name = '({ticker}) {Name}'.format(**content)
            name_list.append(name)
        weight_list = map(lambda x : Paragraph(x,StockTemplate.bullet), weight_list)
        weight_list = list(weight_list)
        name_list = map(lambda x : Paragraph(x,StockTemplate.ticker), name_list)
        name_list = list(name_list)
        logging.info(len(weight_list))
        logging.info(len(name_list))

        row_list = []
        for i, dummy in enumerate(weight_list) :
            row = [weight_list[i], name_list[i]]
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

def process_summary() :
    input_file = EXTRACT.config()
    target = 'summary'
    summary = input_file.get(target,{})
    target = 'images'
    image_list = summary.get(target,[])
    target = 'captions'
    captions_list = summary.get(target,[])
    target = 'table'
    summary_text = summary.get(target,{})

    summary_text = summary_text.replace("'",'"')
    logging.info(summary_text)
    summary_text = loads(summary_text)
    summary_tbl = TRANSFORM.addSummaryTable(summary_text) 

    image_list = map(lambda path : StockTemplate.alter_aspect(path, 3.5 * inch), image_list)
    return image_list, captions_list, summary_tbl

def process_portfolio_list() :
    input_file = EXTRACT.config()
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
        logging.debug(name)
        logging.debug(image_list)
        logging.debug(captions_list)
        logging.debug(diverse_list)
        logging.debug(returns_list)
        yield name, image_list, captions_list, diverse_list, returns_list

@exit_on_exception
@trace
def main() :

    output_file = EXTRACT.instance().output_file
    logging.info('Writing report {}'.format(output_file))
    doc = StockTemplate.initPortrait(output_file)
    toc = StockTemplate.initToc()
    ret = [ toc, PageBreak(), Paragraph('Portfolio Summary', StockTemplate.h1) ]

    images, captions, summary = process_summary() 
    tbl = StockTemplate.addTable(captions,images,[],[])
    ret.append(tbl)
    ret.append(summary)
    ret.append(PageBreak())
 
    for name, images, captions, weights, returns in process_portfolio_list() :
        ret.append(Paragraph(name, StockTemplate.h1))
        tbl = StockTemplate.addTable(captions,images, weights, returns)
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
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*.ini')

   input_file = env.list_filenames('local/report_generator.ini')
   output_file = "{pwd_parent}/local/image.pdf".format(**vars(env))

   main()
