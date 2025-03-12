from bs4 import BeautifulSoup
import requests
import random

# UA池
def get_headers(referer_url):
    first_num = random.randint(55, 76)
    third_num = random.randint(0, 3800)
    fourth_num = random.randint(0, 140)
    os_type = [
        '(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
        '(Macintosh; Intel Mac OS X 10_14_5)'
    ]
    chrome_version = 'Chrome/{}.0.{}.{}'.format(first_num, third_num, fourth_num)

    ua = ' '.join(['Mozilla/5.0', random.choice(os_type), 'AppleWebKit/537.36',
                   '(KHTML, like Gecko)', chrome_version, 'Safari/537.36']
                  )
    headers = {
        "User-Agent": ua,
        "Referer": referer_url
    }
    return headers


def main():
    base_url = 'https://guangzhou.8684.cn'  # 广州公交网的URL

    # 公交线路头字母支持扩展
    bus_head = ['1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'F', 'G', 'H', 'K', 'L',
                'N', 'S', 'T', 'X', 'Y', 'Z']

    for bus in bus_head:
        bus_single_url = f"{base_url}/list{bus}"  # 构造公交线路分类的URL

        try:
            # 获取公交线路分类页面
            resp = requests.get(bus_single_url, headers=get_headers(base_url))
            resp.raise_for_status()  # 检查请求是否成功

            # 解析线路分类页面
            bus_main_html = BeautifulSoup(resp.text, 'html.parser')
            bus_route_list = bus_main_html.find('div', class_="list clearfix").find_all('a')
            route_hrefs = [route.get('href') for route in bus_route_list]

            # 遍历每个公交线路的详细信息
            for href in route_hrefs:
                route_url = f"{base_url}{href}"  # 构造公交线路详细页面的URL

                # 获取公交线路详细页面
                bus_detail = requests.get(route_url, headers=get_headers(bus_single_url))
                bus_detail.raise_for_status()  # 检查请求是否成功

                # 解析线路详细页面
                bus_detail_html = BeautifulSoup(bus_detail.text, 'html.parser')

                try:
                    bus_info = bus_detail_html.find('div', class_="info")
                    # 提取线路基本信息
                    detail = bus_info.get_text('#').split('#')[:6]
                    company_name = detail[-2] + detail[-1]  # 公司名称拼接
                    route_total = bus_detail_html.find_all('div', 'bus-excerpt mb15')
                    bus_lzlist = bus_detail_html.find_all('div', 'bus-lzlist mb15')

                    # 保存基本信息到文件
                    with open('bus.txt', 'a', encoding='utf-8') as f:
                        f.write(f"=== {route_url} 的信息 ===\n")
                        f.write('\n'.join(detail[:4]))  # 写入前4条基本信息
                        f.write('\n' + "公司名称：" + company_name + '\n')

                    # 保存站点信息
                    for route, bus_ls in zip(route_total, bus_lzlist):
                        trip = route.find('div', 'trip').get_text()
                        start, end = trip.split('—')  # 获取起始站点和终点站的名字
                        li_list = [li.get_text() for li in bus_ls.find_all('a')]  # 获取经过站点
                        # 过滤掉重复的首尾站点
                        tmp = [li for idx, li in enumerate(li_list[1:-1]) if li != start and li != end]
                        tmp = [li_list[0]] + tmp + [li_list[-1]]
                        # 添加站点编号
                        tmp = [f'{idx + 1}:{r}' for idx, r in enumerate(tmp)]

                        with open('bus.txt', 'a', encoding='utf-8') as f:
                            f.write('\n' + trip + '\n')
                            f.write(' -> '.join(tmp) + '\n')
                    print(f"{route_url} 的信息写入成功！")

                except Exception as e:
                    # 如果发生解析错误，记录错误信息
                    with open('error.txt', 'a', encoding='utf-8') as f:
                        f.write(f"解析错误：{route_url} - {str(e)}\n")

        except Exception as e:
            # 如果请求线路分类页面失败，记录错误信息
            with open('error.txt', 'a', encoding='utf-8') as f:
                f.write(f"请求错误：{bus_single_url} - {str(e)}\n")


if __name__ == '__main__':
    main()