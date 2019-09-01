
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch    

def horizontalscale(canvas):    
    textobject = canvas.beginText()
    textobject.setTextOrigin(3, 2.5*inch)
    textobject.setFont("Helvetica-Oblique", 12)
    horizontalscale = 80 # 100 is default 
    for line in lyrics: 
        textobject.setHorizScale(horizontalscale)
        textobject.textLine("%s: %s" %(horizontalscale,line))
        horizontalscale = horizontalscale+10
        textobject.setFillColorCMYK(0.0,0.4,0.4,0.2)
    canvas.drawText(textobject)

def fonts(canvas):
    text = "Now is the time for all good men to..."
    x = 1.8*inch
    y = 2.7*inch
    for font in canvas.getAvailableFonts():
        canvas.setFont(font, 10)
        canvas.drawString(x,y,text)
        canvas.setFont("Helvetica", 10)
        canvas.drawRightString(x-10,y, font+":")
        y = y-13

def embedImage(canvas) :
    f = open(path_to_file, 'rb')
    story.append( Image(f) )

c = canvas.Canvas("hello.pdf")
c.drawString(100,750,"Welcome to Reportlab!")
fonts(c)
#horizontalscale(c)    
c.save()
