#/usr/bin/env python

import sys
import logging

from reportlab.pdfgen import canvas
from reportlab.lib.units import inch    

import context
from libCommon import log_on_exception

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

@log_on_exception
def main() :
    target = 'test_dir'
    test_dir = globals().get(target,None)
    target = 'save_file'
    save_file = globals().get(target,None)

    env.mkdir(test_dir)
    c = canvas.Canvas(save_file)
    c.drawString(100,750,"Welcome to Reportlab!")
    fonts(c)
    #horizontalscale(c)    
    c.save()

if __name__ == '__main__' :
   import os,sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = '{pwd_parent}/local'.format(**vars(env))
   test_dir = 'testResults'
   save_file = '{}/hello.pdf'.format(test_dir)

   main()



