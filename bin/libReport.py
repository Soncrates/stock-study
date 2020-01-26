from copy import deepcopy
import logging
from reportlab.lib import utils
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
#from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter, landscape

from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image, TableStyle
from reportlab.platypus import ListFlowable, ListItem
from reportlab.platypus import PageBreak, KeepTogether, Spacer
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle as PS
from reportlab.platypus.frames import Frame
from reportlab.lib.units import cm

def make_portrait(canvas,doc):
    ret = letter
    canvas.setPageSize(ret)
def make_landscape(canvas,doc):
    ret = landscape(letter)
    canvas.setPageSize(ret)
class StockTemplate(BaseDocTemplate):
    default_init = { 'rightMargin' : 72
                   , 'leftMargin' : 72
                   , 'topMargin' : 72
                   , 'bottomMargin' : 72}
    fontName_1 = 'Helvetica-Bold'
    fontName_2 = 'Helvetica'
    styles = getSampleStyleSheet()
    style_caption = PS(name='caption', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, textColor=colors.gray)
    h1 = PS(name = 'Heading1', fontSize = 12, leading = 16, leftIndent = 5 , fontName=fontName_1)
    h2 = PS(name = 'Heading2', fontSize = 10, leading = 14, leftIndent = 10, fontName=fontName_1)
    h3 = PS(name = 'Heading3', fontSize = 10, leading = 12, leftIndent = 15, fontName=fontName_1)
    ticker = PS(name = 'Bullet1', fontSize = 8, leading = 8, alignment=TA_LEFT)
    bullet = PS(name = 'Bullet2', fontSize = 8, leading = 8, alignment=TA_RIGHT)
    _bullet = PS(name = 'Bullet3', fontSize = 8, leading = 6, alignment=TA_LEFT)
    _bulletID = PS(name = 'Bullet4', fontSize = 7, leading = 6, alignment=TA_LEFT, fontName=fontName_1)

    local_ts = [('TOPPADDING', (0,0), (-1,-1), 2),
          ('BOTTOMPADDING', (0,0), (-1,-1), 2),
          ('LEFTPADDING', (0,0), (-1,-1), 1),
          ('RIGHTPADDING', (0,0), (-1,-1), 1),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
          ]
    ts = TableStyle(local_ts)

    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
    def afterFlowable(self, flowable):
        "Registers TOC entries."
        if flowable.__class__.__name__ == 'Paragraph':
            style = flowable.style.name
            text = flowable.getPlainText()
            if style == 'Heading1':
                self.notify('TOCEntry', (0, text, self.page))
            if style == 'Heading2':
                self.notify('TOCEntry', (1, text, self.page))
            if style == 'Heading3':
                self.notify('TOCEntry', (2, text, self.page))
        elif flowable.__class__.__name__ == 'TableOfContents': pass
        elif flowable.__class__.__name__ == 'PageBreak': pass
        elif flowable.__class__.__name__ == 'Table': pass
        else : 
            logging.warn(flowable.__dict__)
            logging.warn(flowable.__class__.__name__)
    @classmethod
    def initPortrait(cls,filename, **kw):
        params = deepcopy(cls.default_init)
        params.update(kw)
        ret = cls(filename,**params)
        width, height = letter
        frame = cls.initFrame(width, height)
        template = PageTemplate(id='portrait',frames =[frame], onPage=make_portrait)
        ret.addPageTemplates(template)
        return ret
    @classmethod
    def initLandScape(cls,filename, **kw):
        params = deepcopy(cls.default_init)
        params.update(kw)
        ret = cls(filename,**params)
        width, height = landscape(letter)
        frame = cls.initFrame(width, height)
        template = PageTemplate(id='landscape',frames =[frame], onPage=make_landscape)
        ret.addPageTemplates(template)
        return ret
    @classmethod
    def initToc(cls):
        ret = TableOfContents()
        # For conciseness we use the same styles for headings and TOC entries
        ret.levelStyles = [cls.h1, cls.h2, cls.h3]
        return ret
    @classmethod
    def initFrame(cls, width=5*cm, height=10*cm):
        ret = Frame(0.05*cm, 0.05*cm, 0.95*width, 0.95*height,id='F1')
        return ret
    @classmethod
    def addTable(cls, name_list, image_list,diverse_list, returns_list ) :
        caption_list = map(lambda caption : Paragraph(caption, cls.style_caption), name_list)
        logging.debug( diverse_list )
        ret = [ image_list , caption_list, [diverse_list, returns_list]]
        #ret = [ image_list , caption_list, [diverse_list,[Paragraph("Blank?",bullet)]]]
        # here you add your rows and columns, these can be platypus objects
        ret = Table(data=ret)
        ret.setStyle(cls.ts)
        return ret

    @classmethod
    def alter_aspect(cls, path, width) :
        im = utils.ImageReader(path)
        w, h = im.getSize()
        logging.debug((w, h))
        aspect = float(width / w)
        logging.info(aspect)
        return Image(path,width, h*aspect)
   
class ReturnsTemplate :
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

    @classmethod
    def initHeaders(cls) :
        ret = ['Name'] + cls.tableHeaders
        ret = map(lambda t : Paragraph(t,StockTemplate._bullet),ret)
        return ret
    @classmethod
    def transformRow(cls, row) :
        key_list = filter(lambda key : key in row, cls.tableHeaders)
        data_row = map(lambda x : row[x], key_list)
        data_row = map(lambda cell : cls.transformCell(cell), data_row)
        return data_row

    @classmethod
    def transformCell(cls, cell) :
        if isinstance(cell, str) : return cell
        return str(round(cell,2))

class SectorTemplate :
    #debugging tables
    ts_debug = [('GRID', (0,0), (-1,-1), 0.25, colors.blue),
          ('TOPPADDING', (0,0), (-1,-1), 1),
          ('BOTTOMPADDING', (0,0), (-1,-1), 1),
          ]
    ts = [('TOPPADDING', (0,0), (-1,-1), 0),
          ('BOTTOMPADDING', (0,0), (-1,-1), 0),
          ('VALIGN',(0,0), (-1,-1), 'TOP'),
          ]
    ts = TableStyle(ts)


if __name__ == "__main__" :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   pdf_landscape = '{pwd_parent}/local/example_landscape.pdf'.format(**vars(env))
   pdf_portrait = '{pwd_parent}/local/example_portrait.pdf'.format(**vars(env))

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)


   story = []
   toc = StockTemplate.initToc()
   story.append(toc)
   story.append(PageBreak())
   story.append(Paragraph('First heading', StockTemplate.h1))
   story.append(Paragraph('Text in first heading', PS('body')))
   story.append(Paragraph('First sub heading', StockTemplate.h2))
   story.append(Paragraph('Text in first sub heading', PS('body')))
   story.append(PageBreak())
   story.append(Paragraph('Second sub heading', StockTemplate.h2))
   story.append(Paragraph('Text in second sub heading', PS('body')))
   story.append(Paragraph('Last heading', StockTemplate.h1))
   doc1 = StockTemplate.initLandScape(pdf_landscape)
   doc1.multiBuild(story)
   doc2 = StockTemplate.initPortrait(pdf_portrait)
   doc2.multiBuild(story)

