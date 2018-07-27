def getLoadingCSS():
	str = '''
		<link rel="stylesheet" href="loading.css" />
	      '''
	return str

def getLoadingHtml():

	str = '''
		<div id='div_loadingImg'>
                        <div id='loadingPage_bg' class='loadingPage_bg1' style="width:100%; height:100%;"></div>
                        <div id='loadingPage'>
                                <section class="bigone right" style="z-index:10005">
                                        <div class="circle">
                                                <div class="cloud">
                                                        <small class="sync"></small>
                                                </div>  
                                                <div class="circle tiny"></div>
                                        </div>
                                </section>
                        </div>
                </div>  
	      '''

	return str


if __name__ == '__main__':
	print getLoadingCSS()
	print getLoadingHtml()
