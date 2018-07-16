import os
import xml.etree.ElementTree as ET
from pyExcelerator import *
os.environ["NLS_LANG"] = ".UTF8"

def file2excel(filename):	
	wb = Workbook()
	ws = wb.add_sheet('sheet1')
	row = 0
	for line in open(filename):
		items = line.split()
		length = len(items)
		for i in range(0,length):
			ws.write(row,i,items[i])
		row += 1
	wb.save(filename+'.xls')

def zyyexcel(xlsname,list1,titles=[]):
	font0 = Font()
	font0.name = 'Verdana'
	font0.struck_out = False
	font0.bold = False
	font0.colour_index = 2

	fnt1 = Font()
	fnt1.name = 'Verdana'
	fnt1.bold = False

        fnt2 = Font()
        fnt2.name = 'Verdana'
        fnt2.bold = False

#	fnt1.height = 18*0x14
	
	pat1 = Pattern()
	pat1.pattern = Pattern.SOLID_PATTERN
	pat1.pattern_fore_colour = 0x16

	brd1 = Borders()
	brd1.left = 0x00
	brd1.right = 0x00
	brd1.top = 0x00
	brd1.bottom = 0x00

	al1 = Alignment()
	al1.horz = Alignment.HORZ_CENTER
	al1.vert = Alignment.VERT_CENTER

        al0 = Alignment()
        al0.horz = Alignment.HORZ_CENTER
        al0.vert = Alignment.VERT_CENTER

        al2 = Alignment()
        al2.horz = Alignment.HORZ_CENTER
        al2.vert = Alignment.VERT_CENTER

        style00 = XFStyle()
        style00.font = fnt1
        style00.alignment = al0
        style00.pattern = pat1
        style00.borders = brd1


	style0 = XFStyle()
	style0.font = font0
	style0.alignment = al0
        style0.pattern = pat1
        style0.borders = brd1

	style1 = XFStyle()
	style1.font = font0
	style1.alignment = al1

        style2 = XFStyle()
        style2.font = fnt1
        style2.alignment = al2
        style2.pattern = pat1
        style2.borders = brd1

        style3 = XFStyle()
        style3.font = fnt1
        style3.alignment = al1


	wb = Workbook()
        ws = wb.add_sheet('sheet1')
        row = 0
	column, n, sign, times = 0, 0, 0, 1
	if len(titles)>0:
		length = len(titles)
		'''
		for i in range(0,length):
			ws.write(row,i,titles[i])
		row+=1	
		'''
	ran = range(0,length)
	num = 0
	total = len(list1)
	rownum = 0
        for line in list1:
	   if len(line) == 3:

		compare = int(line[2])
		s = compare
                length = len(line)-1
		for l in range(0,length):
			tmp = line[l]
			#print tmp
			if type(tmp)==str:
				tmp=line[l].decode('utf8')
			if compare == s and sign == 0:
                       		ws.write(row,ran[l],tmp,style00)
				row += 1
			elif compare != s and sign == 1:
				'''
				try:
					list1[num].index(line[0])
					com = compare - 1
					if com in list1[num]:
						ws.write(row,ran[l],tmp,style0)
					else:
						ws.write(row,ran[l],tmp)
				except:
					ws.write(row,ran[1],tmp)
				'''
				com = compare - 1
				remainder =  times % 2
				if num >= 27:#-----compare-----------
					if (line[0],list1[num-14][1],com) not in list1[num-27:num] and remainder != 0:
						ws.write(row,ran[l],tmp,style0)
                                        elif (line[0],list1[num-14][1],com) not in list1[num-27:num] and remainder == 0:
                                                ws.write(row,ran[l],tmp,style1)
                                        elif (line[0],list1[num-14][1],com) in list1[num-27:num] and remainder != 0:
                                                ws.write(row,ran[l],tmp,style2)
                                        elif (line[0],list1[num-14][1],com) in list1[num-27:num] and remainder == 0:
                                                ws.write(row,ran[l],tmp,style3)
					'''
					elif line[0] in list1[num] :
						ws.write(row,ran[l],tmp,style2)
					'''
				else:
                                        if (line[0],list1[num-14][1],com) not in list1[num-14:num] and remainder != 0:
                                                ws.write(row,ran[l],tmp,style0)
                                        elif (line[0],list1[num-14][1],com) not in list1[num-14:num] and remainder == 0:
                                                ws.write(row,ran[l],tmp,style1)
                                        elif (line[0],list1[num-14][1],com) in list1[num-14:num] and remainder != 0:
                                                ws.write(row,ran[l],tmp,style2)
                                        elif (line[0],list1[num-14][1],com) in list1[num-14:num] and remainder == 0:
                                                ws.write(row,ran[l],tmp,style3)

                row += 1
		num += 1
		'''
	    else:
                length = len(line)-1
                for l in range(0,length):
                        tmp = line[l]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[l].decode('utf8')
                        ws.write(row,ran[l],tmp,style0)
                row += 1
		'''
		if row == 14 and sign == 0:
			n += 1
			row = 0
			ran = range(length*n,length*(n+1))
			if max(ran) == length*3-1:
				times += 1
				row, sign, n  = 14, 1, 0
				ran = range(0,length)	
		elif row == 14*times and sign != 0:
                        n += 1
			row = 14*(times-1)
                        ran = range(length*n,length*(n+1))
                        if max(ran) == length*3-1:
				times += 1
                                row, sign, n  = 14*(times-1), 1, 0
                                ran = range(0,length)
			

				
			
			
        wb.save(xlsname)
        #return wb.tostring()
#--------------- test ------------------

def list2excel_zyy(xlsname,list1,list2,list3,list4,list5,titles1=[],titles2=[],titles3=[],titles4=[],titles5=[]):
        wb = Workbook()
        ws1 = wb.add_sheet('WEB\xd3\xc3\xbb\xa7\xd0\xd0\xce\xaa'.decode('gbk'))
	ws2 = wb.add_sheet('WEB\xb7\xc3\xce\xca\xca\xfd\xbe\xdd'.decode('gbk'))
	ws3 = wb.add_sheet('\xbf\xcd\xbb\xa7\xb6\xcb\xb7\xc3\xce\xca\xca\xfd\xbe\xdd'.decode('gbk'))
	ws4 = wb.add_sheet('\xbb\xfa\xd0\xcd\xcd\xb3\xbc\xc6'.decode('gbk'))
	ws5 = wb.add_sheet('\xbf\xcd\xbb\xa7\xb6\xcb\xd3\xc3\xbb\xa7\xd0\xd0\xce\xaa'.decode('gbk'))
        row = 0
        if len(titles1)>0:
                length = len(titles1)
                for i in range(0,length):
                        ws1.write(row,i,titles1[i])
                row+=1
        for line in list1:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws1.write(row,i,tmp)
                row += 1
	row = 0
        if len(titles2)>0:
                length = len(titles2)
                for i in range(0,length):
                        ws2.write(row,i,titles2[i])
                row+=1

        for line in list2:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws2.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles3)>0:
                length = len(titles3)
                for i in range(0,length):
                        ws3.write(row,i,titles3[i])
                row+=1

        for line in list3:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws3.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles4)>0:
                length = len(titles4)
                for i in range(0,length):
                        ws4.write(row,i,titles4[i])
                row+=1

        for line in list4:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws4.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles5)>0:
                length = len(titles5)
                for i in range(0,length):
                        ws5.write(row,i,titles5[i])
                row+=1
        for line in list5:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws5.write(row,i,tmp)
                row += 1

        wb.save(xlsname)

def list2excel_zyy2(xlsname,list1,list2,list3,list4,titles1=[],titles2=[],titles3=[],titles4=[]):
        wb = Workbook()
        ws1 = wb.add_sheet('\xd2\xbb\xd6\xdc\xd4\xb1\xb9\xa4\xca\xb9\xd3\xc3\xc7\xe9\xbf\xf6'.decode('gbk'))
        ws2 = wb.add_sheet('\xd4\xb1\xb9\xa4\xb7\xa2\xb8\xe5\xc1\xbfTOP50'.decode('gbk'))
        ws3 = wb.add_sheet('\xb8\xf7\xb2\xbf\xc3\xc5\xc4\xda\xb2\xe2\xcd\xb3\xbc\xc6'.decode('gbk'))
	ws4 = wb.add_sheet('\xd2\xbb\xd6\xdc\xd4\xb1\xb9\xa4\xca\xb9\xd3\xc3\xc7\xe9\xbf\xf6\xbb\xe3\xd7\xdc'.decode('gbk'))
        row = 0
        if len(titles1)>0:
                length = len(titles1)
                for i in range(0,length):
                        ws1.write(row,i,titles1[i])
                row+=1
        for line in list1:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws1.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles2)>0:
                length = len(titles2)
                for i in range(0,length):
                        ws2.write(row,i,titles2[i])
                row+=1

        for line in list2:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws2.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles3)>0:
                length = len(titles3)
                for i in range(0,length):
                        ws3.write(row,i,titles3[i])
                row+=1

        for line in list3:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws3.write(row,i,tmp)
                row += 1
        row = 0
        if len(titles4)>0:
                length = len(titles4)
                for i in range(0,length):
                        ws4.write(row,i,titles4[i])
                row+=1
        for line in list4:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
                                tmp=line[i].decode('utf8')
                        ws4.write(row,i,tmp)
                row += 1


	wb.save(xlsname)

def list2excel(xlsname,list1,titles=[]):
        wb = Workbook()
        ws = wb.add_sheet('sheet1')
        row = 0
        if len(titles)>0:
                length = len(titles)
                for i in range(0,length):
                        ws.write(row,i,titles[i])
                row+=1
        for line in list1:
                length = len(line)
                for i in range(0,length):
                        tmp = line[i]
                        #print tmp
                        if type(tmp)==str:
				try:
	                                tmp=line[i].decode('utf8')
				except:
					continue
                        ws.write(row,i,tmp)
                row += 1
        wb.save(xlsname)

def list2excel2(xlsname,list1,list2,titles=[],titles2=[],sheet1='',sheet2=''):
		wb = Workbook()
		'''
        	ws = wb.add_sheet('Baidu->page')
		ws2 = wb.add_sheet('Baidu->Channel')
		'''
                ws = wb.add_sheet(sheet1)
                ws2 = wb.add_sheet(sheet2)
        	row = 0
        	if len(titles)>0:
                	length = len(titles)
                	for i in range(0,length):
                        	ws.write(row,i,titles[i])
                	row+=1
        	for line in list1:
                	length = len(line)
                	for i in range(0,length):
                        	tmp = line[i]
                        	#print tmp
                        	if type(tmp)==str:
                                	tmp=line[i].decode('utf8')
                        	ws.write(row,i,tmp)
                	row += 1
		row = 0
                if len(titles2)>0:
                        length = len(titles2)
                        for i in range(0,length):
                                ws2.write(row,i,titles2[i])
                        row+=1
                for line in list2:
                        length = len(line)
                        for i in range(0,length):
                                tmp = line[i]
                                #print tmp
                                if type(tmp)==str:
                                        tmp=line[i].decode('utf8')
                                ws2.write(row,i,tmp)
                        row += 1

	        wb.save(xlsname)


def obj2excel(xlsname,obj1,titles=[]):
	wb = Workbook()
	for name in obj1:
		ws = wb.add_sheet(name.decode('utf8'))
		
		list1 = obj1[name]
		
		row = 0
        	if len(titles)>0:
                	length = len(titles)
                	for i in range(0,length):
                        	ws.write(row,i,titles[i])
                	row+=1
        	for line in list1:
                	length = len(line)
                	for i in range(0,length):
                        	tmp = line[i]
                        	if type(tmp)==str:
					try:
	                                	tmp=line[i].decode('utf8')
					except:
						continue
				try:
	                        	ws.write(row,i,tmp)
				except:
					continue
                	row += 1
	wb.save(xlsname)

def list2xml(xmlname,list1,names):
	root = ET.Element('root')
	len1 = len(names)
	for line in list1:
                print line
		len2 = len(line)
		if len2!=len1:
			continue
		part = ET.SubElement(root,'part')
		for i in range(0,len2):
			item = ET.SubElement(part, names[i])
        		item.text = str(line[i]).decode('utf8')
			#print item.text
	tree = ET.ElementTree(root)
        return ET.tostring(root)
	
	#f = open(xmlname, 'w')
	#tree.write(f, encoding='utf-8')
	#f.close()
	#tree.write(xmlname)

def getDistXml(date):
	import handler
        depth2uv = handler.getSrcDepthDist2(date)
        root = ET.Element('root')
        len1 = len(depth2uv)
        for depth in depth2uv:
                part = ET.SubElement(root,'part')
                depthnode = ET.SubElement(part, 'depth')
                depthnode.text = str(depth)
                sites = depth2uv[depth]
                for (site,rate) in sites:
                        tmp = site
                        ss = site.split('.')
                        if len(ss) > 1:
                                tmp = ss[0]
                        if tmp == '#':
                                tmp = 'direct'
                        item = ET.SubElement(part, 'S.'+tmp)
                        item.text = str(rate)
                        #print item.text
        tree = ET.ElementTree(root)
	return ET.tostring(root)

def getDistXml2(date):
        depth2uv = handler.getSrcDepthDist3(date)
        root = ET.Element('root')
        len1 = len(depth2uv)
        for depth in depth2uv:
                part = ET.SubElement(root,'part')
                depthnode = ET.SubElement(part, 'depth')
                strdepth = '20+'
                if depth == 1:
                    strdepth = '1'
                elif depth == 2:
                    strdepth = '2~5'
                elif depth == 3:
                    strdepth = '6~10'
                elif depth == 4:
                    strdepth = '11~20'
                depthnode.text = strdepth
                sites = depth2uv[depth]
                for (site,rate) in sites:
                        tmp = site
                        ss = site.split('.')
                        if len(ss) > 1:
                                tmp = ss[0]
                        if tmp == '#':
                                tmp = 'direct'
                        item = ET.SubElement(part, 'S.'+tmp)
                        item.text = str(rate)
                        #print item.text
        tree = ET.ElementTree(root)
	return ET.tostring(root)

def getAdXml(start,end):
    root = ET.Element('root')
    combos = handler.getChannelPV(start,end)
    obj = {}
    for (combo,pv,date) in combos:
        if not obj.has_key(date):
            obj[date] = []
        obj[date].append((combo,pv))

    for date in obj:
        part = ET.SubElement(root,'part')
        datenode = ET.SubElement(part, 'date')
        datenode.text = str(date)
        ss = obj[date]
        for (combo,pv) in ss:
            item = ET.SubElement(part, combo.replace(' ',''))
            item.text = str(pv)

    tree = ET.ElementTree(root)
    return ET.tostring(root)

def generateXml(xmlname,typename,date,enddate,num=10):
       list1,titles=[],[]
       if typename == 'keyword':
              	list1 = handler.getKeywordStat(date,num)
            	titles = ['keyword','uv','visit','pv','depth','dur','exitrate','newrate']
       if typename == 'overall':
                list1 = handler.getOverallStat4long(date,enddate)
            	titles = ['uv','pv','depth','exit','newcomer','datenum']   
       elif typename == 'source':
                list1 = handler.getSourceStat(date,num)
                titles = ['source','uv','visit','pv','depth','dur','exitrate','new_visitor']
       elif typename == 'loc':
                list1 = handler.getLocStat(date,num)
                titles = ['loc','uv','visit','pv','depth','dur','exitrate','newrate']
       elif typename == 'entrance':
                list1 = handler.getUrlStat(date,num)
                titles = ['entrance','uv','visit','pv','depth','dur','exitrate','newrate']
       elif typename == 'exit':
                list1 = handler.getExitStat(date,num)
                titles = ('url','pv','exit','exitrate')
       elif typename == 'dst':
                list1 = handler.getUrlkeyStat(date,num)
                titles = ('url','uv','source','srcuv','keyword')
       elif typename == 'chs':
                #list1 = handler.getChannelPV(date,enddate)
                #titles = ('combo','pv','date')
                return getAdXml(date,enddate)
       elif typename == 'chstotal':
                list1 = handler.getChannelPV4long(date,enddate)
                titles = ('combo','pv')
       elif typename == 'adchs':
                list1 = handler.getAdChannelPV(date,enddate)
                titles = ('channel','pv')
       elif typename == 'cms_src':
                list1 = handler.getCmsSourceStat(date,num)
                titles = ('source','articles','upv','pv')
       elif typename == 'src_var':
                list1 = handler.getSrcMonitor(date,num)
                titles = ('source','uv','yesterdayUV','lastweekUV','lastweekAvg','lastmonthAvg')
       elif typename == 'depth':
			return getDistXml(date)
       elif typename == 'depth2':
			return getDistXml2(date)
       #xmlname = '/opt/yangming/twisted/htm/tmp.xml'
       return list2xml(xmlname,list1,titles)


def generateXls(xlsname,typename,date,reportType,num=10):
       import dateutil
       dates = []
       if reportType == 'weekly':
           dates = dateutil.getWeekdates(date)
       elif reportType == 'monthly':
           dates = dateutil.getMonthdates(date)
       else:
           dates.append(date)
       startdate = dates[0]
       enddate = dates[-1]
       list1,titles,result=[],[],[]
       if typename == 'keyword':
              	list1 = handler.getKeywordStat(date,num)
              	for (keyword,uv,visit,pv,depth,dur,exitrate,newrate) in list1:
                        result.append((keyword,uv,pv,depth))
                list1 = result
            	titles = ['keyword','uv','pv','depth']
       elif typename == 'source':
                for dt in dates:
                    list1 = handler.getSourceStat(dt,100)
                    for (src,uv,visit,pv,depth,dur,exitrate,new_visitor) in list1:
                        result.append((dt,src,uv,pv,new_visitor))
                list1 = result
                titles = ['datenum','source','uv','pv','new_visitor']
       elif typename == 'loc':
                list1 = handler.getLocStat4long(startdate,enddate,num)
                for (loc,uv,pv) in list1:
                    result.append((loc,uv,pv))
                list1 = result
                titles = ['loc','uv','pv']
       elif typename == 'overall':
                list1 = handler.getOverallStat4long(startdate,enddate,num)
                for (loc,uv,pv) in list1:
                    result.append((loc,uv,pv))
                list1 = result
                titles = ['loc','uv','pv']
       elif typename == 'entrance':
                list1 = handler.getUrlStat(date,num)
                titles = ['entrance','uv','visit','pv','depth','dur','exitrate','newrate']
       elif typename == 'exit':
                list1 = handler.getExitStat(date,num)
                titles = ('url','pv','exit','exitrate')
       elif typename == 'chs':
                list1 = handler.getChannelPV4long(startdate,enddate)
                titles = ('combo','pv')
       elif typename == 'dst':
                list1 = handler.getUrlkeyStat(date,num)
                titles = ('url','uv','source','srcuv','keyword')
       elif typename == 'cms_src':
                list1 = handler.getCmsSourceStat4long(startdate,enddate,num)
                titles = ('source','articles','pv_excludePages','pv')
       elif typename == 'depth':
			return getDistXml(date)
       elif typename == 'depth2':
                list1 = handler.getSrcDepthDist(date,num)
                titles = ('source','depth','uv')
       #xmlname = '/opt/yangming/twisted/htm/tmp.xml'
       return list2excel(xlsname,list1,titles)

if __name__ == '__main__':
	#filename = 'channelInfo.log'
	#file2excel(filename)
	import os,analysis
	os.environ["NLS_LANG"] = ".UTF8"
	datenum = 20090825
	xlsname = '/home/bi/channels_refer_20090916.xls'
	titles = ['type','refer','pv']
	obj1 = analysis.getChannelRefer2('2009-09-16')
	obj2excel(xlsname,obj1,titles)	
