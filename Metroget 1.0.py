# encoding:utf-8
import warnings
# 过滤 transbigdata 中因添加 geometry 列而触发的 FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)

import requests
import json
import transbigdata as tbd

# 接口地址
url = "https://api.map.baidu.com/place/v2/search"

# 获取用户输入
ak = input("请输入百度key：")
radius = int(input('请输入半径，例如1000米，则输入1000即可：'))
location = input("请输出经纬度坐标：")
page_max = int(input("请输入最大页面："))

# 分割经纬度，并重组为“经度,纬度”格式
latitude, longitude = location.split(',')
new_location = f"{longitude.strip()}, {latitude.strip()}"

page = 0  # 用于翻页
all_results = []  # 初始化结果列表

# 循环获取所有页面数据
while page < page_max:
    params = {
        "query": "地铁站",
        "location": new_location,
        "radius": radius,
        "output": "json",
        "ak": ak,
        "page_size": 10,
        "page_num": page,  # 页码从0开始
    }
    response = requests.get(url=url, params=params)
    response_json = response.text  # 提取响应文本
    dic = json.loads(response_json)  # 将 JSON 字符串转为字典格式

    data = dic['results']
    all_results += data  # 将当前页数据加入结果列表

    page += 1  # 更新页码
    print(f"第{page}页...")

# 保存原始数据到文件
with open('bus_poi.json', 'w', encoding='utf-8') as file:
    json.dump({"results": all_results}, file, ensure_ascii=False, indent=4)

# 创建 GeoJSON 的 FeatureCollection 结构
geojson = {
    "type": "FeatureCollection",
    "features": []
}

bus_names = []
for result in all_results:
    bus_names.append(result["name"])
    # 将百度坐标转换为 WGS84 坐标
    wgs84_lng, wgs84_lat = tbd.bd09towgs84(result["location"]["lng"], result["location"]["lat"])
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [wgs84_lng, wgs84_lat]
        },
        "properties": {
            "name": result["name"],
            "address": result["address"],
            "province": result["province"],
            "city": result["city"],
            "area": result["area"],
            "uid": result["uid"]
        }
    }
    geojson["features"].append(feature)

# 保存 GeoJSON 数据到文件
with open('bus_poi.geojson', 'w', encoding='utf-8') as file:
    json.dump(geojson, file, ensure_ascii=False, indent=4)

# 处理线路信息
bus_lines = [result['address'] for result in all_results]

# 拆分、去重线路信息
separated_lines = []
for line in bus_lines:
    separated_lines.extend(line.split(';'))

unique_lines_set = set(separated_lines)
unique_lines_list = list(unique_lines_set)

# 输出结果
print(f'目标点范围内共有 {len(bus_names)} 个公交站点和 {len(unique_lines_list)} 条公交线路')
print(bus_names)
print(unique_lines_list)

# 读取公交线路信息（包含线路和站点）
city = input('请确认项目所在的城市：')
lines, stops = tbd.getbusdata(city, unique_lines_list)

# ------------------------------
# 设置活动几何列与 CRS
# ------------------------------
# 显式指定 GeoDataFrame 的活动几何列，避免未来版本自动设定变化的风险
lines = lines.set_geometry('geometry')
stops = stops.set_geometry('geometry')
# 明确赋予 CRS 为 EPSG:4326，确保写入的 GeoJSON 包含投影信息
lines.set_crs("EPSG:4326", inplace=True)
stops.set_crs("EPSG:4326", inplace=True)

# 保存为 GeoJSON 文件
lines.to_file("lines.geojson", driver="GeoJSON")
stops.to_file("stops.geojson", driver="GeoJSON")
