#from reportlab.lib import utils
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
#from reportlab.lib.units import inch
#from reportlab.lib.pagesizes import letter

from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Image, TableStyle
from reportlab.platypus import ListFlowable, ListItem
from reportlab.platypus import PageBreak, KeepTogether, Spacer
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle as PS
from reportlab.platypus.frames import Frame
from reportlab.lib.units import cm


class StockTemplate(BaseDocTemplate):
    styles = getSampleStyleSheet()
    style_caption = PS(name='caption', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, textColor=colors.gray)
    h1 = PS(name = 'Heading1', fontSize = 12, leading = 16, leftIndent = 5 , fontName='Helvetica-Bold')
    h2 = PS(name = 'Heading2', fontSize = 10, leading = 14, leftIndent = 10, fontName='Helvetica-Bold')
    h3 = PS(name = 'Heading3', fontSize = 10, leading = 12, leftIndent = 15, fontName='Helvetica-Bold')
    ticker = PS(name = 'Bullet1', fontSize = 8, leading = 8, alignment=TA_LEFT)
    bullet = PS(name = 'Bullet2', fontSize = 8, leading = 8, alignment=TA_RIGHT)
    _bullet = PS(name = 'Bullet3', fontSize = 8, leading = 6, alignment=TA_LEFT)
    _bulletID = PS(name = 'Bullet4', fontSize = 7, leading = 6, alignment=TA_LEFT, fontName='Helvetica-Bold')
    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
        template = PageTemplate('normal', [Frame(2.5*cm, 2.5*cm, 15*cm, 25*cm, id='F1')])
        self.addPageTemplates(template)
    def afterFlowable(self, flowable):
        "Registers TOC entries."
        if flowable.__class__.__name__ == 'Paragraph':
            text = flowable.getPlainText()
            style = flowable.style.name
            if style == 'Heading1':
                self.notify('TOCEntry', (0, text, self.page))
            if style == 'Heading2':
                self.notify('TOCEntry', (1, text, self.page))
            if style == 'Heading3':
                self.notify('TOCEntry', (2, text, self.page))
    @classmethod
    def initToc(cls):
        ret = TableOfContents()
        # For conciseness we use the same styles for headings and TOC entries
        ret.levelStyles = [cls.h1, cls.h2, cls.h3]
        return ret
   
if __name__ == "__main__" :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   pdf = '{pwd_parent}/local/{name}_example.pdf'.format(**vars(env))


   # Build story.
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
   doc = StockTemplate(pdf)
   doc.multiBuild(story)

