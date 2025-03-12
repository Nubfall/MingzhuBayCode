# encoding:utf-8
import requests
import json
import transbigdata as tbd

# 接口地址
url = "https://api.map.baidu.com/place/v2/search"

# 获取用户输入
ak = input("请输入百度key：")
radius = int(input('请输入半径，例如1000米，则输入1000即可：'))
location = input("请输出经纬度坐标：")
page_max=int(input("请输入最大页面："))

# 分割经纬度
latitude, longitude = location.split(',')

# 重组经纬度为“经度,纬度”格式
new_location = f"{longitude.strip()}, {latitude.strip()}"
page = 0  # 用于翻页


# 初始化结果列表
all_results = []


# 循环获取所有页面数据
while page < page_max:
    # 初始参数
    params = {
        "query": "公交车站",
        "location": new_location,
        "radius": radius,
        "output": "json",
        "ak": ak,
        "page_size": 10,
        "page_num": page,  # 初始页码设为1
    }
    response = requests.get(url=url, params=params)
    response_json = response.text  # 提取响应文本  
    dic = json.loads(response_json)  # 因为接收的数据是json字符串格式，所以把它转为字典格式

    data = dic['results']
    all_results=all_results + data

    # 更新页码
    page += 1
    print(f"第{page}页...")

# 保存数据到文件
with open('bus_poi.json', 'w', encoding='utf-8') as file:
    json.dump({"results": all_results}, file, ensure_ascii=False, indent=4)

# 创建GeoJSON的FeatureCollection结构
geojson = {
    "type": "FeatureCollection",
    "features": []
}

bus_names = []
for result in all_results:
    bus_names.append(result["name"])
    wgs84_lng,wgs84_lat=tbd.bd09towgs84(result["location"]["lng"],result["location"]["lat"])
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

# 保存GeoJSON到文件
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

# 读取线路信息
city = input('请确认项目所在的城市：')
lines, stops = tbd.getbusdata(city, unique_lines_list)

# 保存为GeoJSON文件
lines.to_file("lines.geojson", driver="GeoJSON")
stops.to_file("stops.geojson", driver="GeoJSON")