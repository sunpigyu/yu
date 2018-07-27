from StringIO import StringIO
import urllib2
import gzip
import time
import sys,os
import db
import get_day
from GetLoadingJS import *

def ifeng():
	day_old = get_day.getMaxDayFromRt_wap_doc_ch()
	day = time.strftime('%Y-%m-%d %H:%M:00',time.localtime(time.time()))
	day_d = day_old.split(' ')[0]
	day_h = day_old.split(' ')[-1][:5]
	day_dd = day_d.replace('-','')
	day_hh = day_h.replace(':','')
	day_new_hh = day.split(' ')[-1][:5].replace(':','')

	#get rt page




	page_xml = os.system("curl -o /data/index_sys/webs/wapnew/www_index_xml/%s.html.gz https://i.ifeng.com"%day_hh)

	os.system("rm -f /data/index_sys/webs/wapnew/www_index_xml/%s.html"%day_hh)


	os.system("gunzip  /data/index_sys/webs/wapnew/www_index_xml/%s.html.gz"%day_hh)

	os.system("rm -f /data/index_sys/webs/wapnew/www_index_xml/%s.html.gz"%day_hh)



	page=open('/data/index_sys/webs/wapnew/www_index_xml/'+day_hh+'.html').read()
	time_ss_1 = day.split(' ')[-1].split(':')[0]
	time_ss_2 = day.split(' ')[-1].split(':')[1]
	f = open('/data/index_sys/webs/wapnew/www_index_new/ifeng.html','w')

	try:
	#	page = page.replace('sta_collection','')
		pageArr = page.split('$.get(URL);')
		page = pageArr[0] + pageArr[1]
	except:
		pass

	#loading animation begin
        load_body0 = page.split('</head>')[0]
        load_body1 = page.split('<body>')[1]
        style = getLoadingCSS()
        body = getLoadingHtml()
        page = load_body0+style+'</head><body style="max-width:500px;margin-left: auto;margin-right: auto;">'+body+load_body1
	page = page.replace('<script type="text/javascript" src="http://p1.ifengimg.com/buyAlbum/js/ifengad/20170601201/fm-ad.js"></script>','')
        page = page.replace('<script src="http://c0.ifengimg.com/pdt/cfg/ad/js/20160812/mobile_inice_v1.js"></script>','')
        #loading animation end
	con,cur=db.open_15()
	list_new = []
	try:
		cur.execute("select replace(url,'http','https')url,sum(uv),max(page_uv) from wapnew.rt_index_doc_zhishu where day = to_date('%s','yyyy-mm-dd hh24:mi:ss') and page = 'http://i.ifeng.com/' group by url"%day_old)
		for every in cur.fetchall():
		#	print day
			url = every[0].strip()
			url = url.strip('"')
			uv = every[1]
			page_uv = every[2]
			list_new.append((url,uv,page_uv))
		#	print url,'\t',uv
	except Exception,e:
		print e
	json = '		var json = {"result":['
	for every2 in list_new:
		rate = str("%.2f"%(float(every2[1]*100)/every[2]))+'%'
		json = json + '{"url":"' + every2[0] + '","uv":"' + str(every2[1]) + '","rate":"' + rate + '"},'
	json = json.strip(',')
	json = json + ']};'
	js1 = '''
<style>
.start{
	max-width:500px;
	margin-left: auto;
	margin-right: auto;
}
.tongjiclickstyle {
	padding:1px 0px;
	background-image: linear-gradient(to right, rgb(190, 247, 215), rgba(255, 255, 255, 0.14));
	float: left;
	font-family: Arial, Helvetica, sans-serif;
	color: rgba(255, 0, 0, 0.75);
	font-size: 13px;
/*  opacity: 0.7;*/	
	z-Index:8000;
	vertical-align: middle;
	cursor:pointer;
	border-radius: 2px;
	text-shadow: 1px 1px 0 #fff;
	line-height:12px;
}
.tongjiclickstyleHighlight {
	background-color: rgba(224, 255, 253, 0.7);
    	float: left;
    /*opacity: 0.8;*/
	color:red;
	padding:4px;
	font-size:16px;
	min-width:100px;
	z-index:9050;
}
.tplidxtimediv {
	font-family:Arial, Helvetica, sans-serif;
	background: url(Camera.gif) no-repeat 108px 9px;
	border-radius: 0px 20px 20px 0px;
	text-align: center;
	background-color: #8dc03c;
	color: #fff;
	z-Index: 9000;
	vertical-align: middle;
	padding:15px 0px 0px 0px;	
	text-shadow: 1px 1px 0 #999;
}
.tplidxtime {
	font-weight:normal;
	color: rgb(255, 255, 255);
	font-size: 32px;
	margin:0px 8px 0px 3px;
	padding-bottom:0px;
}
.tplidxtopdiv {
	background-color: rgb(255, 255, 255);
	opacity: 0.8;
	z-Index: 9000;
	vertical-align: middle;
}
.tplidxdescdiv {
	background-image: url(clickdata_info.gif);
	z-Index: 8000;
}
.tplidxbg1 {
	background-image: linear-gradient(to right, rgb(230, 230, 230), rgba(255, 255, 255, 0.14));
	}
.tplidxbg2 {
	background-image: linear-gradient(to right, rgb(199, 247, 190), rgba(255, 255, 255, 0.14));
}
.tplidxbg3 {
	background-image: linear-gradient(to right, rgb(230, 240, 121), rgba(255, 255, 255, 0.14));
}
.tplidxbg4 {
	background-image: linear-gradient(to right, rgb(247, 217, 190), rgba(255, 255, 255, 0.14));
}
.tplidxlabel{
	margin-top: -40px;
}

</style>

<script src="http://ipushms.ifengcdn.com/ipush/res/js/jquery.min.js"></script>
<script>
var $jq = jQuery.noConflict();
</script>
<iframe id="iframeC" name="iframeC" src="" width="0" height="0" style="display:none;" ></iframe>
<script>
function dealWindows(){
  var topValue;
  var leftValue;
  var flag = 0;
  var numLi = 12 ;
  function remParamFromUrl(params,url){
    url = url.replace(/((#imgnum)|(#p))=\d/g,'');
    if(url.indexOf('?') > -1){
      var _stayArr = [];
      var _parStr = url.split('?')[1];
      var _parArr = _parStr.split('&');
      for(var j=0; j<params.length ; j++){
        for(var i =(_parArr.length-1) ; i>=0 ; i--){
          var param = params[j];
          if(_parArr[i].split('=')[0] === param){
            _stayArr.push(_parArr[i]);
            break;
          }
        }
      }
      var returnUrl = '';
      if(_stayArr.length==0){
        var urlBefore = url.split('?')[0];
        if(urlBefore.substr(urlBefore.length-4,4)=='.com'){
          returnUrl = url.split('?')[0]+'/';
        }else{
          returnUrl = url.split('?')[0];
        }
      }else{
        returnUrl = url.split('?')[0] + '?' + _stayArr.join('&');
      }
      if(returnUrl.indexOf('http://')<0){
        returnUrl = "http://i.ifeng.com"+returnUrl;
      }
      return returnUrl;
    }else{
      return url;
    }
  };
	function renderIdx(idx,index){
		idx = $jq(idx);
		var url = idx.attr("href");
		if(!url) return;
		if(url.substr(0,1)=="/"){
      idx.attr("href","http://i.ifeng.com"+url);
    }
'''
	js2 =  '''
		var rate = "";
		var uv = "";
		var result = json.result; 
		for (var i in result){
          if (json.result[i].url == remParamFromUrl(['aid','bid','liveId'],url)){
                  idx[0].href = remParamFromUrl(['aid','bid','liveId'],url);
                  uv = json.result[i].uv||"";
                  rate = json.result[i].rate||"";
                  break;
          }
		}
		'''
	js3 = '''		
	var label = idx.first();
		if(uv==""){
		 /* if(idx.parent().is('li')){
                    if(idx.hasClass('i_con')){
			numLi-=1;
			if(numLi==0){
                        numLi=12;
			}
                }
		    } */
			
		return;
			
		} 

		var bgclass = "tplidxbg1";
		var num = rate.substr(0,rate.length-1);
		if(num >= 10){
			bgclass = "tplidxbg4";
		} else if(num >=5 && num < 10){
			bgclass = "tplidxbg3";
		} else if(num >=1 && num < 5){
			bgclass = "tplidxbg2";
		}
    var maskEl;
    var pos = label.offset();
    var w = label.outerWidth(),h=label.outerHeight();
    idx[0].id = "tongjiTest"+flag;
    if(idx.parent().hasClass('i_topNews_slide')){
      w = idx.children('img').outerWidth();
      h = idx.children('img').outerHeight();
      var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ h +'px;margin-top:'+ (-h) +'px;position:relative" class="tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      idx.parent().append(maskEl);
    }else if(idx.parent().is('li')){
    if(idx.hasClass('i_con')){
                var h1=numLi*h;
                numLi-=1;
                if(numLi==0){
                        numLi=12;
                }
    /* var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ h +'px;margin-top:'+ (-h1) +'px;position:relative" class="tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
                  idx.parent().append(maskEl);*/
 var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ (h<30?30:h) +'px;top:'+ pos.top +'px;left:'+ pos.left +'px;position:absolute" class="absolute4tongji tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      $jq('body').append(maskEl);
        }else{
      var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ h +'px;margin-top:'+ (-h) +'px;position:relative" class="tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      idx.parent().append(maskEl);
      }
    }else if(idx.parent('div.ifgBoxPic').length>0){
      var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:470px;height:300px;margin-top:-308px;position:relative" class="tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      idx.parent().append(maskEl);
    }else if(idx.parent('div.ifgBoxVidPic').length>0){
      w = idx.children('img')[0].clientWidth;
      h = idx.children('img')[0].clientHeight;
      var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ (h>300?265:h) +'px;margin-top:'+ (h>300?-265:-h) +'px;position:relative" class="tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      idx.parent().append(maskEl);
    }else{
      var maskEl =$jq('<div id = "tongjiTag'+flag+'" style="width:'+ w +'px;height:'+ (h<30?30:h) +'px;top:'+ pos.top +'px;left:'+ pos.left +'px;position:absolute" class="absolute4tongji tongjiclickstyle ' + bgclass + '"><b style="font-family:Arial, Helvetica, sans-serif;font-weight:bold">'+ rate +'</b> <span id="tplidx_uv" style="display:none;font-family:Arial, Helvetica, sans-serif;">'+ uv +'</span></div>'); 
      $jq('body').append(maskEl);
    }
    flag+=1;
    $jq(maskEl).mouseover(function(e){
      $jq(this).addClass("tongjiclickstyleHighlight");
      $jq(this).find('#tplidx_uv').css("display","");
    }).mouseout(function(e){
      $jq(this).removeClass("tongjiclickstyleHighlight");
      $jq(this).find('#tplidx_uv').css("display","none");
    }).click(url,function(e){
      if(e.data.indexOf('http://')<0){
         window.open("http://i.ifeng.com"+e.data);
      }else{
         window.open(e.data);
      }
    });
	};
	function addTime(){
		var label = $jq($jq('.tempWrap')[0]);
		var pos = label.offset();
		var w = label.outerWidth(),h=label.outerHeight();
                 console.info('-----width:'+w+',height:'+h);
		var maskEl =$jq('<div style="width:'+ 130 +'px;height:'+ 70 + 'px;top:'+ (pos.top+45) +'px;left:'+ (pos.left+500) +'px;position:fixed;" class="tplidxtimediv" title="\xe6\x95\xb0\xe6\x8d\xae\xe6\x97\xb6\xe9\x97\xb4"><p class="tplidxtime">'+' '''
	js4 = ''' '+'</p><p style="font-family:Arial, Helvetica, sans-serif;">'+' '''
	js5 = ''' [1 min]'+'</p></div>'); 
		$jq('body').append(maskEl);
	};
function init(){
		var idxs = $jq('a[href]');
		for(var i=0;i<idxs.length;i++){
			try {
        renderIdx(idxs[i],i);
      }catch (e) {
      }
		}
	}
	$jq($jq('body').find('.w7F9T81')[0]).remove();
        $jq($jq('body').find('#iis_yw1')).remove();
	try {
			addTime();
		}catch (e) {
			console.log(e);
	}
       try {
            init();            
                }catch (e1) {
                        console.log(e1);
        }

	//$('#div_loadingImg').hide();
};
function timeout(){
    //hashH = document.documentElement.scrollHeight;
    //urlC = "http://index.tongji.ifeng.com/ieye/clickdataMiddlePagev3.html";
    //document.getElementById("iframeC").src=urlC+"#"+hashH;
   // var eyeDiv = document.getElementById('eyeDiv');
   // if(eyeDiv){
   //   eyeDiv.style.display='none';
  // }
    // $('#ifgNavBox').remove();
    // $('#video-cmwap').remove();
    // $('#video-ctwap').remove();
    // $('#video-unicom').remove();
     $jq('#div_loadingImg').hide();
    $('#iis2_if_16469').parent('div').remove();
    // var navigationDiv = $($('.ifgNav.wrap').find('div')[0]);
    // navigationDiv.height(navigationDiv.height()*navigationDiv.find('ul').length);
    // $('#slNavBox').remove();
     setTimeout("dealWindows()", 300);
};
 $jq(document).ready(timeout);
</script>
</body>
</html>
<!--pagebottomprobe-->'''
	js6 = js1 + json + js2 + js3 + day_h +js4 + js5 
	page_all = page + js6
	print >>f,page_all
	con.close() 


if __name__ == "__main__":
	ifeng()
               
