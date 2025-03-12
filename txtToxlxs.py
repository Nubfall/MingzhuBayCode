import re
import pandas as pd
from pathlib import Path
import sys

def install_package(package):
    """自动安装缺失依赖"""
    print(f"正在安装必要依赖: {package}")
    try:
        import subprocess
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
    except:
        print(f"安装失败，请手动执行: pip install {package}")
    sys.exit(1)

try:
    import xlsxwriter
except ImportError:
    install_package('xlsxwriter')

def extract_route_name(line):
    """从线路名称中提取有效信息"""
    patterns = [
        r"(.*)公交车路线",
        r"([\u4e00-\u9fa5]+\d+路)",
        r"(.+?)(?:$.*线路$)"
    ]
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            return match.group(1).strip()
    return line.strip()

def extract_times(s: str):
    """
    从字符串中提取所有符合 \d{1,2}:\d{1,2} 的时间，返回原始顺序的列表。
    """
    raw_times = re.findall(r'(\d{1,2}:\d{1,2})', s)
    return raw_times  # 直接返回原始顺序

def time_to_decimal(time_str):
    """将时间字符串（如'08:30'）转换为小数（如8.5）"""
    if not time_str:
        return None
    try:
        hours, minutes = map(int, time_str.split(':'))
        return hours + minutes / 60.0
    except ValueError:
        return None

def process_time(time_str):
    """
    结合多条特殊规则解析运行时间字符串，返回(始发站, 首班车, 末班车)。
    """
    if "到站立刻返程" in time_str and '|' in time_str:
        time_str = time_str.split('|')[0]
    if "增加停靠" in time_str:
        time_str = re.sub(r'\([^)]*\)', '', time_str)

    weekday_keywords = [
        "工作日", "工作日、周六", "节假日", "周一至周五", "周六日", "周六、日、节假日", "周五或节假日前一天",
        "每周二、周四", "上学日周日或法定节假日最后一日", "上学日周五或法定节假日前一日",
        "节假日前", "节假日后"
    ]

    fb_pattern = r"(发班时间|发车时间)"
    if re.search(fb_pattern, time_str):
        match_fb = re.search(fb_pattern, time_str)
        fb_index = match_fb.start()
        station = time_str[:fb_index].strip()
        colon_split = re.split(r'[：:]', time_str[fb_index:], maxsplit=1)
        if len(colon_split) >= 2:
            times_part = colon_split[1].strip()
            times = extract_times(times_part)
            if times:
                return station, times[0], times[-1]

    if any(k in time_str for k in weekday_keywords):
        times = extract_times(time_str)
        if times:
            station_part = time_str.split(times[0])[0].strip()
            return station_part, times[0], times[-1]

    if "到站立刻返程" in time_str:
        times = extract_times(time_str)
        if times:
            station_part = time_str.split(times[0])[0].strip()
            return station_part, times[0], times[-1]

    if "增加停靠" in time_str:
        times = extract_times(time_str)
        if times:
            station_part = time_str.split(times[0])[0].strip()
            return station_part, times[0], times[-1]

    dash_match = re.search(r'(\d{1,2}:\d{1,2})\s*--\s*(\d{1,2}:\d{1,2})', time_str)
    if dash_match:
        station_part = time_str.split(dash_match.group(1))[0].strip()
        first_bus = dash_match.group(1)
        last_bus = dash_match.group(2)
        return station_part, first_bus, last_bus
    else:
        times = extract_times(time_str)
        if times:
            station_part = time_str.split(times[0])[0].strip()
            return station_part, times[0], times[-1]
        else:
            return "未知站点", "06:00", "22:30"

def parse_stations_from_section(section):
    """
    解析某个区块中的站点列表，排除无关信息。
    """
    stations = []
    skip_keywords = [
        "公交车路线", "[市区编码线路]", "运行时间：", "参考票价：",
        "公司名称：", "票价", "公交公司："
    ]
    for line in section:
        if any(kw in line for kw in skip_keywords):
            continue
        cleaned = re.sub(r'\d+[:.]?\s*', '', line)
        parts = re.split(r'->|→', cleaned)
        stations.extend([s.strip() for s in parts if s.strip()])
    return stations

def parse_bus_data(file_path):
    """综合处理：解析线路名称、时间，并区分正向与反向的站点"""
    if not Path(file_path).exists():
        raise FileNotFoundError(f"文件 {file_path} 不存在")

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    forward_data = []
    reverse_data = []
    line_counter = 0

    blocks = re.split(r'=== https://.*? ===', content)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        try:
            lines = [l.strip() for l in block.split('\n') if l.strip()]
            if len(lines) < 5:
                continue

            route_name = extract_route_name(lines[0])
            if not route_name:
                continue
            line_counter += 1

            company = next((line.split("公交公司：")[-1] for line in lines if "公交公司：" in line), "未知公司")

            time_info = next((line.replace('运行时间：', '') for line in lines if line.startswith('运行时间：')), "")
            time_parts = time_info.split('|')

            header_indices = [i for i, line in enumerate(lines) if ("—" in line) and ("公交" not in line)]
            if len(header_indices) >= 2:
                forward_section = lines[header_indices[0] + 1 : header_indices[1]]
                reverse_section = lines[header_indices[1] + 1 : ]
                forward_stations = parse_stations_from_section(forward_section)
                reverse_stations = parse_stations_from_section(reverse_section)
            else:
                forward_stations = parse_stations_from_section(lines)
                reverse_stations = forward_stations[::-1] if len(forward_stations) > 1 else []

            if forward_stations:
                f_station, f_start, f_end = process_time(time_parts[0] if time_parts else "")
                forward_data.append({
                    "线路编号": line_counter,
                    "线路名称": route_name,
                    "运营公司": company,
                    "始发站": f_station,
                    "首班车": f_start,
                    "末班车": f_end,
                    "途经站点": ",".join(forward_stations),
                    "首班车（小数制）": time_to_decimal(f_start),
                    "末班车（小数制）": time_to_decimal(f_end)
                })

            if reverse_stations:
                r_str = time_parts[1] if len(time_parts) > 1 else (time_parts[0] if time_parts else "")
                r_station, r_start, r_end = process_time(r_str)
                reverse_data.append({
                    "线路编号": line_counter,
                    "线路名称": route_name,
                    "运营公司": company,
                    "始发站": r_station,
                    "首班车": r_start,
                    "末班车": r_end,
                    "途经站点": ",".join(reverse_stations),
                    "首班车（小数制）": time_to_decimal(r_start),
                    "末班车（小数制）": time_to_decimal(r_end)
                })

        except Exception as e:
            print(f"线路 {line_counter} 解析失败，错误：{str(e)}")
            continue

    return pd.DataFrame(forward_data), pd.DataFrame(reverse_data)

def generate_excel(output_path, forward_df, reverse_df):
    """生成带完整站点信息的Excel文件"""
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            columns = ["线路编号", "线路名称", "运营公司", "始发站", "首班车", "末班车", "首班车（小数制）", "末班车（小数制）", "途经站点"]

            forward_df.to_excel(
                writer,
                sheet_name='正向线路',
                index=False,
                columns=columns
            )

            reverse_df.to_excel(
                writer,
                sheet_name='反向线路',
                index=False,
                columns=columns
            )

            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#E6E6FA',
                'border': 1
            })
            time_format = workbook.add_format({'num_format': '@'})
            decimal_format = workbook.add_format({'num_format': '0.00'})

            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:A', 10)
                worksheet.set_column('B:B', 25)
                worksheet.set_column('C:C', 20)
                worksheet.set_column('D:D', 25)
                worksheet.set_column('E:F', 12, time_format)
                worksheet.set_column('G:H', 15, decimal_format)
                worksheet.set_column('I:I', 100)
                worksheet.freeze_panes(1, 0)
                for col_num, value in enumerate(columns):
                    worksheet.write(0, col_num, value, header_format)

    except Exception as e:
        print(f"文件生成失败：{str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    input_file = "guangzhou_bus_data.txt"
    output_file = "广州公交线路详情.xlsx"

    try:
        print("正在解析数据...")
        forward_df, reverse_df = parse_bus_data(input_file)

        print("生成Excel文件中...")
        generate_excel(output_file, forward_df, reverse_df)

        print(f"成功生成文件：{output_file}")
        print("数据统计：")
        print(f"正向线路记录数：{len(forward_df)}")
        print(f"反向线路记录数：{len(reverse_df)}")
        if not forward_df.empty:
            print(f"途经站点示例：{forward_df.iloc[0]['途经站点'][:50]}...")
    except Exception as e:
        print(f"运行出错：{str(e)}")
        print("排查建议：")
        print("1. 确认输入文件格式符合要求")
        print("2. 检查文件编码是否为UTF-8")
        print("3. 验证示例数据是否包含完整线路信息")