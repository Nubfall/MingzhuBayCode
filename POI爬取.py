import shapefile as sp
import requests as re
import json
import pandas as pd
import time
import os
import sys
import atexit
from decimal import Decimal, getcontext

# 设置高精度（可根据需要调整精度）
getcontext().prec = 12

# 自定义异常，当所有 key 均额度不足时抛出
class QuotaExhaustedError(Exception):
    pass

# 全局变量用于批量爬取进度（仅在功能3中使用）
progress_data = {}
batch_mode = False  # 标识是否在批量爬取模式下

# 进度保存与恢复函数（针对功能3）
def save_progress(progress_data, filename="progress.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=4)
    print("当前进度已保存至", filename)

def load_progress(filename="progress.json"):
    with open(filename, "r", encoding="utf-8") as f:
        progress_data = json.load(f)
    print("已加载进度文件", filename)
    return progress_data

# atexit处理函数，若在批量爬取模式下且未完成，则自动保存当前进度
def force_save_progress():
    global batch_mode, progress_data
    if batch_mode and progress_data:
        save_progress(progress_data)

atexit.register(force_save_progress)

# 全局变量：存放多个 key 以及当前使用的 key 指针
KEY_LIST = []
current_key_index = 0

def get_current_key():
    global KEY_LIST, current_key_index
    return KEY_LIST[current_key_index]

def rotate_key():
    global current_key_index, KEY_LIST
    current_key_index = (current_key_index + 1) % len(KEY_LIST)
    print("切换至新 key：", get_current_key())

def multi_key_request(url: str, params: dict) -> dict:
    """
    采用轮询方式请求：
    - 每次请求使用当前 key，然后立即 rotate 到下一个 key；
    - 如果返回结果 status 为 "0" 且 infocode 为 "10003"（每日额度超限），
      则该次请求继续尝试下一 key，直到所有 key 均尝试过为止。
    """
    tried = 0
    while tried < len(KEY_LIST):
        current = get_current_key()
        params['key'] = current
        response = re.get(url, params=params)
        result = json.loads(response.text)
        # 每次请求后立即轮询到下一个 key
        rotate_key()
        if result.get('status') == '1':
            return result
        elif result.get('status') == '0' and result.get('infocode') == "10003":
            print("Key", current, "超出每日限额，尝试下一个 key。")
            tried += 1
            time.sleep(0.2)
            continue
        else:
            return result
    raise QuotaExhaustedError("所有 key 均已用完额度，请稍后再试或添加新 key。")

# ------------------- 各接口函数 -------------------

def poibian1(bianjiestr: str, guanjianci: str, leixing: str) -> dict:
    params = {
        'polygon': bianjiestr,
        'types': leixing,
        'keywords': guanjianci,
        'page_size': '25',
        'show_fields': 'children,business,indoor,navi,photos'
    }
    return multi_key_request("https://restapi.amap.com/v5/place/polygon?parameters", params)

def poibian0(bianjiestr: str, guanjianci: str, leixing: str) -> dict:
    params = {
        'polygon': bianjiestr,
        'types': leixing,
        'keywords': guanjianci,
        'page_size': '25'
    }
    return multi_key_request("https://restapi.amap.com/v5/place/polygon?parameters", params)

def Judge1(ax: float, ay: float, bx: float, by: float, guanjianci: str, leixing: str):
    bianjiestr = f"{ax},{ay}|{bx},{by}"
    res = poibian1(bianjiestr, guanjianci, leixing)
    count = len(res.get('pois', []))
    if count >= 25:
        print(count, "no")
        return 0
    else:
        print(count, "yes")
        return res

def Judge0(ax: float, ay: float, bx: float, by: float, guanjianci: str, leixing: str):
    bianjiestr = f"{ax},{ay}|{bx},{by}"
    res = poibian0(bianjiestr, guanjianci, leixing)
    count = len(res.get('pois', []))
    if count >= 25:
        print(count, "no")
        return 0
    else:
        print(count, "yes")
        return res

def bianma(address: str) -> dict:
    params = {'address': address}
    return multi_key_request("https://restapi.amap.com/v3/geocode/geo?parameters", params)

def poidian1(zuobiao: str, banjing: str, guanjianci: str, leixing: str) -> dict:
    params = {
        'location': zuobiao,
        'radius': banjing,
        'keywords': guanjianci,
        'types': leixing,
        'page_size': '25',
        'show_fields': 'children,business,navi,photos'
    }
    return multi_key_request("https://restapi.amap.com/v5/place/around?parameters", params)

def poidian0(zuobiao: str, banjing: str, guanjianci: str, leixing: str) -> dict:
    params = {
        'location': zuobiao,
        'radius': banjing,
        'keywords': guanjianci,
        'types': leixing,
        'page_size': '25'
    }
    return multi_key_request("https://restapi.amap.com/v5/place/around?parameters", params)

# ------------------- 主程序 -------------------
gongneng = int(input('选择你需要的功能，依据需要的功能输入对应数字并按回车：\n'
                     '1：获取地理编码\n'
                     '2：按半径查询POI\n'
                     '3：按shp查询POI（获取大于25个点的数据）\n'))
# 输入多个 key（用英文逗号分隔）
key_input = input('请输入你的多个 key（用英文逗号分隔）：\n')
KEY_LIST = [k.strip() for k in key_input.split(',') if k.strip() != '']

print('如果下面某项不需要填写，请直接按回车')

# ------------------- 功能1：获取地理编码 -------------------
if gongneng == 1:
    flag = 1
    while flag == 1:
        yuanshi = pd.DataFrame({})
        address = input('输入你要查询的地点（建议填写结构化地址信息：省份＋城市＋区县＋城镇＋乡村＋街道＋门牌号码+关键词）\n'
                        '示例：成都市成华区二仙桥东三路1号成都理工大学\n')
        CurBianma = bianma(address)
        ResBianma = {'名称': [], '等级': [], '坐标': [], '城市': [], '区县': []}
        yuanshi = pd.concat([yuanshi, pd.DataFrame([CurBianma])], ignore_index=True)
        for i in range(len(CurBianma.get('geocodes', []))):
            geocode = CurBianma['geocodes'][i]
            ResBianma['名称'].append(geocode.get('formatted_address', ''))
            ResBianma['等级'].append(geocode.get('level', ''))
            ResBianma['坐标'].append(geocode.get('location', ''))
            ResBianma['城市'].append(geocode.get('city', ''))
            ResBianma['区县'].append(geocode.get('district', ''))
            print(f"第{i+1}项 名称：{ResBianma['名称'][i]}  等级：{ResBianma['等级'][i]}  坐标：{ResBianma['坐标'][i]}  城市：{ResBianma['城市'][i]}  区县：{ResBianma['区县'][i]}")
        flag = int(input('是否再次查询？（1：是 / 0：否）\n'))
        shuchu = pd.DataFrame(ResBianma)
        shuchu.to_excel('地理编码.xlsx', sheet_name='全部', index=False)
        yuanshi.to_excel('地理编码_原始获取.xlsx', sheet_name='全部', index=False)
        print('结果已生成在程序根目录下的“地理编码.xlsx”和“地理编码_原始获取.xlsx”文件')

# ------------------- 功能2：按半径查询POI -------------------
elif gongneng == 2:
    flag = 1
    while flag == 1:
        yuanshi = pd.DataFrame({})
        zuobiao = input('输入目标经纬度（格式：经度,纬度，例如104.14753100,30.67501200）\n')
        banjing = input('输入搜索半径（单位：米）\n')
        guanjianci = input('输入关键词（只支持一个）\n')
        leixing = input('输入你需要的POI类型（参考POI分类编码和城市编码表；多个类型用“|”分割；默认指定餐饮服务、生活服务、商务住宅）\n')
        detail_mode = int(input('是否需要详细信息？（1：详细 / 0：简要）\n'))
        if detail_mode == 1:
            CurDian = poidian1(zuobiao, banjing, guanjianci, leixing)
            if CurDian.get('status') == '1':
                yuanshi = pd.concat([yuanshi, pd.DataFrame([CurDian])], ignore_index=True)
            else:
                print("未获取有效响应：", CurDian.get('info', ''))
            ResDian = {'poi 名称': [], 'poi 唯一标识': [], 'poi 经纬度': [], 'poi 所属类型': [], 'poi 分类编码': [],
                       'poi 所属省份': [], 'poi 所属城市': [], 'poi 所属区县': [],
                       'poi 详细地址': [], 'poi 所属省份编码': [], 'poi 所属区域编码': [], 'poi 所属城市编码': [],
                       'poi 营业时间': [],
                       '子 poi 唯一标识': [], '子 poi 名称': [], '子 poi 经纬度': [], '子 poi 详细地址': [],
                       '子 poi 所属类型': [], '子 poi 分类编码': [],
                       'poi 所属商圈': [], 'poi 的联系电话': [], 'poi 特色内容': [], 'poi 评分': [],
                       'poi 人均消费': [], '停车场类型': [], 'poi 的别名': [],
                       'poi 对应的导航引导点坐标': [], 'poi 的入口经纬度坐标': [],
                       'poi 的出口经纬度坐标': [], 'poi 的地理格 id': [],
                       'poi 的图片介绍': [], 'poi 图片的下载链接': []}
            print(CurDian)
            for poi in CurDian.get('pois', []):
                ResDian['poi 名称'].append(poi.get('name', ''))
                ResDian['poi 唯一标识'].append(poi.get('id', ''))
                ResDian['poi 经纬度'].append(poi.get('location', ''))
                ResDian['poi 所属类型'].append(poi.get('type', ''))
                ResDian['poi 分类编码'].append(poi.get('typecode', ''))
                ResDian['poi 所属省份'].append(poi.get('pname', ''))
                ResDian['poi 所属城市'].append(poi.get('cityname', ''))
                ResDian['poi 所属区县'].append(poi.get('adname', ''))
                ResDian['poi 详细地址'].append(poi.get('address', ''))
                ResDian['poi 所属省份编码'].append(poi.get('pcode', ''))
                ResDian['poi 所属区域编码'].append(poi.get('adcode', ''))
                ResDian['poi 所属城市编码'].append(poi.get('citycode', ''))
                # 新增：poi营业时间，从business中获取opentime_week字段
                ResDian['poi 营业时间'].append(poi.get('business', {}).get('opentime_week', ''))
                try:
                    ResDian['子 poi 唯一标识'].append(poi['children']['id'])
                except:
                    ResDian['子 poi 唯一标识'].append('')
                try:
                    ResDian['子 poi 名称'].append(poi['children']['name'])
                except:
                    ResDian['子 poi 名称'].append('')
                try:
                    ResDian['子 poi 经纬度'].append(poi['children']['location'])
                except:
                    ResDian['子 poi 经纬度'].append('')
                try:
                    ResDian['子 poi 详细地址'].append(poi['children']['address'])
                except:
                    ResDian['子 poi 详细地址'].append('')
                try:
                    ResDian['子 poi 所属类型'].append(poi['children']['subtype'])
                except:
                    ResDian['子 poi 所属类型'].append('')
                try:
                    ResDian['子 poi 分类编码'].append(poi['children']['typecode'])
                except:
                    ResDian['子 poi 分类编码'].append('')
                try:
                    ResDian['poi 所属商圈'].append(poi['business']['business_area'])
                except:
                    ResDian['poi 所属商圈'].append('')
                try:
                    ResDian['poi 的联系电话'].append(poi['business']['tel'])
                except:
                    ResDian['poi 的联系电话'].append('')
                try:
                    ResDian['poi 特色内容'].append(poi['business']['tag'])
                except:
                    ResDian['poi 特色内容'].append('')
                try:
                    ResDian['poi 评分'].append(poi['business']['rating'])
                except:
                    ResDian['poi 评分'].append('')
                try:
                    ResDian['poi 人均消费'].append(poi['business']['cost'])
                except:
                    ResDian['poi 人均消费'].append('')
                try:
                    ResDian['停车场类型'].append(poi['business']['parking_type'])
                except:
                    ResDian['停车场类型'].append('')
                try:
                    ResDian['poi 的别名'].append(poi['business']['alias'])
                except:
                    ResDian['poi 的别名'].append('')
                try:
                    ResDian['poi 对应的导航引导点坐标'].append(poi['navi']['navi_poiid'])
                except:
                    ResDian['poi 对应的导航引导点坐标'].append('')
                try:
                    ResDian['poi 的入口经纬度坐标'].append(poi['navi']['entr_location'])
                except:
                    ResDian['poi 的入口经纬度坐标'].append('')
                try:
                    ResDian['poi 的出口经纬度坐标'].append(poi['navi']['exit_location'])
                except:
                    ResDian['poi 的出口经纬度坐标'].append('')
                try:
                    ResDian['poi 的地理格 id'].append(poi['navi']['gridcode'])
                except:
                    ResDian['poi 的地理格 id'].append('')
                try:
                    ResDian['poi 的图片介绍'].append(poi['photos'][0]['title'])
                except:
                    ResDian['poi 的图片介绍'].append('')
                try:
                    ResDian['poi 图片的下载链接'].append(poi['photos'][0]['url'])
                except:
                    ResDian['poi 图片的下载链接'].append('')
                print("POI名称：", ResDian['poi 名称'][-1],
                      " 坐标：", ResDian['poi 经纬度'][-1],
                      " 类型：", ResDian['poi 所属类型'][-1],
                      " 地址：", ResDian['poi 详细地址'][-1],
                      " 营业时间：", ResDian['poi 营业时间'][-1],
                      " 城市：", ResDian['poi 所属城市'][-1],
                      " 区县：", ResDian['poi 所属区县'][-1])
        else:
            CurDian = poidian0(zuobiao, banjing, guanjianci, leixing)
            if CurDian.get('status') == '1':
                yuanshi = pd.concat([yuanshi, pd.DataFrame([CurDian])], ignore_index=True)
            else:
                print("未获取有效响应：", CurDian.get('info', ''))
            ResDian = {'名称': [], '坐标': [], 'POI类型': [], '详细地址': [], '城市': [], '区县': []}
            for poi in CurDian.get('pois', []):
                ResDian['名称'].append(poi.get('name', ''))
                ResDian['坐标'].append(poi.get('location', ''))
                ResDian['POI类型'].append(poi.get('type', ''))
                ResDian['详细地址'].append(poi.get('address', ''))
                ResDian['城市'].append(poi.get('cityname', ''))
                ResDian['区县'].append(poi.get('adname', ''))
                print("POI名称：", ResDian['名称'][-1],
                      " 坐标：", ResDian['坐标'][-1],
                      " 类型：", ResDian['POI类型'][-1],
                      " 地址：", ResDian['详细地址'][-1],
                      " 城市：", ResDian['城市'][-1],
                      " 区县：", ResDian['区县'][-1])
        flag = int(input('是否再次查询？（1/0）\n'))
        shuchu = pd.DataFrame(ResDian)
        if detail_mode == 1:
            shuchu.to_excel('按半径查询POI_详细信息.xlsx', sheet_name='全部', index=False)
            yuanshi.to_excel('按半径查询POI_详细信息_原始获取.xlsx', sheet_name='全部', index=False)
            print('“按半径查询POI_详细信息.xlsx”和“按半径查询POI_详细信息_原始获取.xlsx”已生成')
        else:
            shuchu.to_excel('按半径查询POI_简要信息.xlsx', sheet_name='全部', index=False)
            yuanshi.to_excel('按半径查询POI_简要信息_原始获取.xlsx', sheet_name='全部', index=False)
            print('“按半径查询POI_简要信息.xlsx”和“按半径查询POI_简要信息_原始获取.xlsx”已生成')

# ------------------- 功能3：按shp查询POI（批量爬取） -------------------
elif gongneng == 3:
    guanjianci = input('输入关键词：\n')
    leixing = input('输入poi类型：\n')
    input('请将目标shp文件放置于程序同一文件夹内，文件名需命名为“目标区域.shp”，坐标系要求为GCJ02，完成后按回车...')
    MaxPol = sp.Reader('目标区域.shp')
    MaxPolBox = MaxPol.bbox
    MaxPolBoxFlo = [round(float(MaxPolBox[0]), 6), round(float(MaxPolBox[1]), 6),
                    round(float(MaxPolBox[2]), 6), round(float(MaxPolBox[3]), 6)]
    ResPolList = []
    # 初始查询区域队列，后续可能会细分
    CurPolList = [MaxPolBoxFlo]
    i = 0
    yuanshi = pd.DataFrame({})
    # 开启批量爬取模式，便于在退出时强制保存进度
    batch_mode = True
    detail_mode = int(input('是否需要详细信息？（1：详细 / 0：简要）（这里必须选择1，不然会出问题）\n'))
    if detail_mode == 1:
        ResList = {'poi 名称': [], 'poi 唯一标识': [], 'poi 经纬度': [], 'poi 所属类型': [], 'poi 分类编码': [],
                   'poi 所属省份': [], 'poi 所属城市': [], 'poi 所属区县': [],
                   'poi 详细地址': [], 'poi 所属省份编码': [], 'poi 所属区域编码': [], 'poi 所属城市编码': [],
                   'poi 营业时间': [],
                   '子 poi 唯一标识': [], '子 poi 名称': [], '子 poi 经纬度': [], '子 poi 详细地址': [],
                   '子 poi 所属类型': [], '子 poi 分类编码': [],
                   'poi 所属商圈': [], 'poi 的联系电话': [], 'poi 特色内容': [], 'poi 评分': [],
                   'poi 人均消费': [], '停车场类型': [], 'poi 的别名': [],
                   '是否有室内地图标志': [], '所在建筑物的 POI ID': [],
                   '楼层索引': [], '所在楼层': [],
                   'poi 对应的导航引导点坐标': [], 'poi 的入口经纬度坐标': [],
                   'poi 的出口经纬度坐标': [], 'poi 的地理格 id': [],
                   'poi 的图片介绍': [], 'poi 图片的下载链接': []}
    else:
        ResList = {'名称': [], '坐标': [], 'POI类型': [], '详细地址': [], '城市': [], '区县': []}

    progress_file = "progress.json"
    # 若上次有保存进度，则询问是否恢复
    if os.path.exists(progress_file):
        resume_choice = input("检测到之前保存的进度，是否恢复？ (1/0): ")
        if resume_choice.strip() == "1":
            progress = load_progress(progress_file)
            CurPolList = progress["CurPolList"]
            ResPolList = progress["ResPolList"]
            ResList = progress["ResList"]
            i = progress["i"]
            KEY_LIST = progress["KEY_LIST"]
            current_key_index = progress["current_key_index"]
        else:
            os.remove(progress_file)
            CurPolList = [MaxPolBoxFlo]
            ResPolList = []
            i = 0

    print("开始查询区域矩形数：", len(CurPolList))
    try:
        while i < len(CurPolList):
            ax, ay, bx, by = CurPolList[i]
            if detail_mode == 1:
                CurPol = Judge1(ax, ay, bx, by, guanjianci, leixing)
            else:
                CurPol = Judge0(ax, ay, bx, by, guanjianci, leixing)
            if type(CurPol) == int:
                # 若当前矩形返回结果为0（即POI数>=25），则细分当前矩形为四个小矩形
                CurPolList.append([ax, ay, round((ax + bx) / 2, 6), round((ay + by) / 2, 6)])
                CurPolList.append([round((ax + bx) / 2, 6), ay, bx, round((ay + by) / 2, 6)])
                CurPolList.append([ax, round((ay + by) / 2, 6), round((ax + bx) / 2, 6), by])
                CurPolList.append([round((ax + bx) / 2, 6), round((ay + by) / 2, 6), bx, by])
            else:
                if CurPol.get('status') == '1':
                    yuanshi = pd.concat([yuanshi, pd.DataFrame([CurPol])], ignore_index=True)
                else:
                    print("当前矩形未获得有效响应：", CurPol.get('info', ''))
                if detail_mode == 1:
                    for poi in CurPol.get('pois', []):
                        ResList['poi 名称'].append(poi.get('name', ''))
                        ResList['poi 唯一标识'].append(poi.get('id', ''))
                        ResList['poi 经纬度'].append(poi.get('location', ''))
                        ResList['poi 所属类型'].append(poi.get('type', ''))
                        ResList['poi 分类编码'].append(poi.get('typecode', ''))
                        ResList['poi 所属省份'].append(poi.get('pname', ''))
                        ResList['poi 所属城市'].append(poi.get('cityname', ''))
                        ResList['poi 所属区县'].append(poi.get('adname', ''))
                        ResList['poi 详细地址'].append(poi.get('address', ''))
                        ResList['poi 所属省份编码'].append(poi.get('pcode', ''))
                        ResList['poi 所属区域编码'].append(poi.get('adcode', ''))
                        ResList['poi 所属城市编码'].append(poi.get('citycode', ''))
                        # 新增：poi营业时间，从business中获取opentime_week字段
                        ResList['poi 营业时间'].append(poi.get('business', {}).get('opentime_week', ''))
                        try:
                            ResList['子 poi 唯一标识'].append(poi['children']['id'])
                        except:
                            ResList['子 poi 唯一标识'].append('')
                        try:
                            ResList['子 poi 名称'].append(poi['children']['name'])
                        except:
                            ResList['子 poi 名称'].append('')
                        try:
                            ResList['子 poi 经纬度'].append(poi['children']['location'])
                        except:
                            ResList['子 poi 经纬度'].append('')
                        try:
                            ResList['子 poi 详细地址'].append(poi['children']['address'])
                        except:
                            ResList['子 poi 详细地址'].append('')
                        try:
                            ResList['子 poi 所属类型'].append(poi['children']['subtype'])
                        except:
                            ResList['子 poi 所属类型'].append('')
                        try:
                            ResList['子 poi 分类编码'].append(poi['children']['typecode'])
                        except:
                            ResList['子 poi 分类编码'].append('')
                        try:
                            ResList['poi 所属商圈'].append(poi['business']['business_area'])
                        except:
                            ResList['poi 所属商圈'].append('')
                        try:
                            ResList['poi 的联系电话'].append(poi['business']['tel'])
                        except:
                            ResList['poi 的联系电话'].append('')
                        try:
                            ResList['poi 特色内容'].append(poi['business']['tag'])
                        except:
                            ResList['poi 特色内容'].append('')
                        try:
                            ResList['poi 评分'].append(poi['business']['rating'])
                        except:
                            ResList['poi 评分'].append('')
                        try:
                            ResList['poi 人均消费'].append(poi['business']['cost'])
                        except:
                            ResList['poi 人均消费'].append('')
                        try:
                            ResList['停车场类型'].append(poi['business']['parking_type'])
                        except:
                            ResList['停车场类型'].append('')
                        try:
                            ResList['poi 的别名'].append(poi['business']['alias'])
                        except:
                            ResList['poi 的别名'].append('')
                        try:
                            ResList['poi 对应的导航引导点坐标'].append(poi['navi']['navi_poiid'])
                        except:
                            ResList['poi 对应的导航引导点坐标'].append('')
                        try:
                            ResList['poi 的入口经纬度坐标'].append(poi['navi']['entr_location'])
                        except:
                            ResList['poi 的入口经纬度坐标'].append('')
                        try:
                            ResList['poi 的出口经纬度坐标'].append(poi['navi']['exit_location'])
                        except:
                            ResList['poi 的出口经纬度坐标'].append('')
                        try:
                            ResList['poi 的地理格 id'].append(poi['navi']['gridcode'])
                        except:
                            ResList['poi 的地理格 id'].append('')
                        try:
                            ResList['poi 的图片介绍'].append(poi['photos'][0]['title'])
                        except:
                            ResList['poi 的图片介绍'].append('')
                        try:
                            ResList['poi 图片的下载链接'].append(poi['photos'][0]['url'])
                        except:
                            ResList['poi 图片的下载链接'].append('')
                        # 为未处理的字段补充默认值
                        ResList['是否有室内地图标志'].append('')
                        ResList['所在建筑物的 POI ID'].append('')
                        ResList['楼层索引'].append('')
                        ResList['所在楼层'].append('')
                        print("POI名称：", ResList['poi 名称'][-1],
                              " 坐标：", ResList['poi 经纬度'][-1],
                              " 类型：", ResList['poi 所属类型'][-1],
                              " 地址：", ResList['poi 详细地址'][-1],
                              " 营业时间：", ResList['poi 营业时间'][-1],
                              " 城市：", ResList['poi 所属城市'][-1],
                              " 区县：", ResList['poi 所属区县'][-1])
                else:
                    for poi in CurPol.get('pois', []):
                        ResList['名称'].append(poi.get('name', ''))
                        ResList['坐标'].append(poi.get('location', ''))
                        ResList['POI类型'].append(poi.get('type', ''))
                        ResList['详细地址'].append(poi.get('address', ''))
                        ResList['城市'].append(poi.get('cityname', ''))
                        ResList['区县'].append(poi.get('adname', ''))
                ResPolList.append(CurPolList[i])
                if CurPol.get('status') == '1':
                    yuanshi = pd.concat([yuanshi, pd.DataFrame([CurPol])], ignore_index=True)
                else:
                    print("当前矩形未获得有效响应：", CurPol.get('info', ''))
            print(f"当前在查询第 {i+1} 个矩形，队列总数：{len(CurPolList)}，当前矩形：{CurPolList[i]}")
            i += 1
            time.sleep(0.3)
    except QuotaExhaustedError as e:
        print("错误：", str(e))
        save_choice = input("检测到所有 key 额度已用完，是否保存当前进度？ (1/0): ")
        if save_choice.strip() == "1":
            progress_data = {
                "CurPolList": CurPolList,
                "ResPolList": ResPolList,
                "ResList": ResList,
                "i": i,
                "KEY_LIST": KEY_LIST,
                "current_key_index": current_key_index
            }
            save_progress(progress_data)
        else:
            print("未保存进度。")
        sys.exit(0)

    if os.path.exists(progress_file):
        os.remove(progress_file)

    res1 = pd.DataFrame(ResPolList)
    res2 = pd.DataFrame(ResList)
    print(res2)
    # 拆分详细信息中位置字段（若存在“poi 经纬度”，否则拆分“坐标”字段）
    if 'poi 经纬度' in res2.columns:
        res2['经度'] = res2['poi 经纬度'].apply(lambda x: Decimal(x.split(',')[0].strip()) if isinstance(x, str) and ',' in x else None)
        res2['纬度'] = res2['poi 经纬度'].apply(lambda x: Decimal(x.split(',')[1].strip()) if isinstance(x, str) and ',' in x else None)
    elif '坐标' in res2.columns:
        res2['经度'] = res2['坐标'].apply(lambda x: Decimal(x.split(',')[0].strip()) if isinstance(x, str) and ',' in x else None)
        res2['纬度'] = res2['坐标'].apply(lambda x: Decimal(x.split(',')[1].strip()) if isinstance(x, str) and ',' in x else None)

    if detail_mode == 1:
        res2.to_excel('按shp查询POI_详细信息.xlsx', sheet_name='全部', index=False)
    else:
        res2.to_excel('按shp查询POI_简要信息.xlsx', sheet_name='全部', index=False)
    yuanshi.to_excel('按shp查询POI_原始获取.xlsx', sheet_name='全部', index=False)
    print("输出结果已生成在程序根目录下。")



#嵌入展开表程序
import pandas as pd
import ast
import json

def process_pois(row):
    """将pois字符串转换为有效字典列表"""
    try:
        # 处理单引号问题并验证数据结构
        str_data = row["pois"].replace("'", '"')
        parsed = json.loads(str_data)
        # 过滤非字典元素
        return [item for item in parsed if isinstance(item, dict)]
    except Exception as e:
        try:
            # 备用解析方案
            parsed = ast.literal_eval(row["pois"])
            # 二次过滤确保元素类型
            return [item for item in parsed if isinstance(item, dict)]
        except:
            print(f"解析失败的行已过滤：{row.name}")
            return []  # 返回空列表会被后续处理过滤

# 读取原始文件
df = pd.read_excel("按shp查询POI_原始获取.xlsx")

# 转换pois列为标准列表（包含类型验证）
df["pois"] = df.apply(process_pois, axis=1)

# 展开数据并过滤空值
df_expanded = (
    df.explode("pois")
    .reset_index(drop=True)
    .dropna(subset=["pois"])  # 过滤展开后的空值
)

# 二次验证数据结构
df_expanded = df_expanded[df_expanded["pois"].apply(lambda x: isinstance(x, dict))]

# 转换为标准化JSON字符串
df_expanded["pois"] = df_expanded["pois"].apply(
    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None
)

# 最终清理（防止转换产生的空值）
df_clean = df_expanded.dropna(subset=["pois"])

# 保存结果
df_clean.to_excel("new_完整POI信息.xlsx", index=False)
print("处理完成，有效记录数：", len(df_clean))

#嵌入拆分POI程序

import pandas as pd
from ast import literal_eval
from pandas import json_normalize

# 读取Excel文件，并确保 pois 列以字符串形式读取
df = pd.read_excel('new_完整POI信息.xlsx', sheet_name='Sheet1', dtype={'pois': str})

# 将 pois 列的字符串转换为字典
try:
    df['pois'] = df['pois'].apply(literal_eval)
except Exception as e:
    # 处理单引号问题
    df['pois'] = df['pois'].str.replace("'", '"').apply(pd.io.json.loads)

# 展平嵌套结构
normalized_df = json_normalize(df['pois'])

# 处理 location 字段（新增）
if 'location' in normalized_df.columns:
    # 确保 location 为字符串并去除换行和空格
    normalized_df['location'] = normalized_df['location'].astype(str).str.strip()
    # 拆分成经度和纬度列
    loc_split = normalized_df['location'].str.split(',', expand=True)
    normalized_df['longitude'] = loc_split[0].str.strip()
    normalized_df['latitude'] = loc_split[1].str.strip()
    # 删除原 location 列
    normalized_df.drop('location', axis=1, inplace=True)


# 处理 photos 字段（优化）
def extract_photos_urls(photos):
    if isinstance(photos, list):
        return ';'.join([p.get('url', '') for p in photos])
    return ''


if 'photos' in normalized_df.columns:
    normalized_df['photos.urls'] = normalized_df['photos'].apply(extract_photos_urls)
    normalized_df.drop('photos', axis=1, inplace=True)

# 确保经纬度字段为字符串
normalized_df['longitude'] = normalized_df['longitude'].astype(str)
normalized_df['latitude'] = normalized_df['latitude'].astype(str)

# 使用 xlsxwriter 引擎保存 Excel 文件，并设置经纬度列为文本格式
with pd.ExcelWriter('输出文件.xlsx', engine='xlsxwriter') as writer:
    normalized_df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # 获取 longitude 和 latitude 的列索引（注意 Excel 中列索引从 0 开始）
    lon_idx = normalized_df.columns.get_loc('longitude')
    lat_idx = normalized_df.columns.get_loc('latitude')

    # 设置文本格式，防止 Excel 自动转换
    text_format = workbook.add_format({'num_format': '@'})
    worksheet.set_column(lon_idx, lon_idx, None, text_format)
    worksheet.set_column(lat_idx, lat_idx, None, text_format)

print("处理完成！结果已保存到 输出文件.xlsx")

input('程序运行结束，按回车退出')