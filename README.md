# MingzhuBayCode
一些做项目时用到的代码，基于研究区广州市南沙区明珠湾  
部分代码来自互联网（原谅我已经找不到出处了...  
代码绝代部分由AI生成，请自行调试  

使用方法：  
1.运行Guangzhou8464.py,获取广州市所有公共交通路线（包括地铁）。  
2.运行txtToxlxs.py，将获取的公交信息文件转换为xlxs标准格式文件。  
3.运行Busget 1.0.py、Metroget 1.0.py(代码来源：公众号 Hello Trans)，获取公交线路和公交站、地铁线路及地铁站。详情请见https://mp.weixin.qq.com/s/uHctt3bdVj0wcchdwKN5UA  

2025.03.19新增：  
1.POI爬取工具.py:推荐使用3：按shp查询POI（获取大于25个点的数据），打印信息中选择1：详细。    

2025.04.07新增:  
1.获取街区（交通小区）的py代码get_MSDCW_street-blocks。  
2.代码来源：Tang J, Xu L, Yu H, et al. A dataset of multi-level street-block divisions of 985 cities worldwide[J]. Scientific Data, 2025, 12(1): 456.  
3.对代码进行部分改动以适应项目需要。  
4.需要图层：（1）Open Street Map路网road.shp，需要有fclass字段；（2）admin.shp文件，需要有Name_0(countryname)、Name(citiname)字段.  

2025.04.09新增：  
1.新增 NSGA-II 算法GeneticAlgorithm.py(一种改进的遗传算法)，相较于传统的遗传算法具有（1）快速非支配排序（2）多样性保护等优点。详情请见：https://blog.csdn.net/weixin_45526117/article/details/128507020  
2.代码内的部分实验数据可联系nubfall@outlook.com获取。  
3.实现功能：  
（1）调用ThreadPoolExecutor实现并发操作；  
（2）调用arcpy库，实现shp读取、网络分析获取旅行时间、计算覆盖率等，并定义优化目标；  

依赖安装：  
pip install requests transbigdata BeautifulSoup4 Path xlsxwriter shapely scikit-base pyproj pyogrio pykalman geopandas osmnx  
