import arcpy
import pandas as pd
import math
import random
import os
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 设置初始文件夹路径并创建文件地理数据库
workspace_folder = r"E:\Pycharm\GA"
arcpy.env.workspace = workspace_folder
gdb_name = "GA_Workspace.gdb"
gdb_path = arcpy.CreateFileGDB_management(workspace_folder, gdb_name).getOutput(0)

# 设置工作空间为新创建的文件地理数据库
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

# 网络数据集路径（保持不变，确保路径有效）
network_dataset_path = r"E:\Pycharm\GA\data\RoadNetwork.gdb\RoadNetwork\RoadNetwork_ND"

# 辅助函数：数据加载与网络属性检查
def load_data(file_path, fields):
    if not arcpy.Exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    return arcpy.da.SearchCursor(file_path, fields)

def list_network_attributes(network_dataset_path):
    desc = arcpy.Describe(network_dataset_path)
    attributes = [attr.name for attr in desc.attributes]
    print("可用网络属性:", attributes)
    return attributes

# 数据加载
bus_stop_path = r"E:\Pycharm\GA\data\MingzhuBay_Bustop.shp"
fields = [f.name for f in arcpy.ListFields(bus_stop_path)]
if "FID" not in fields:
    raise ValueError("FID 字段不存在，请检查数据格式")
bus_stops = load_data(bus_stop_path, ["SHAPE@XY", "FID"])
bus_stops_list = [(row[0][0], row[0][1], row[1]) for row in bus_stops]
bus_stops_df = pd.DataFrame(bus_stops_list, columns=["X", "Y", "FID"])

# 街区数据（例如街区合并后的 shp）
district_path = r"E:\Pycharm\GA\data\StreetBlock_Dissolve.shp"
districts = load_data(district_path, ["SHAPE@XY"])
districts_list = [row[0] for row in districts]
districts_df = pd.DataFrame(districts_list, columns=["X", "Y"])

# OD 点数据（三个时段），数据中包含 "weight" 字段
od_period1_path = r"E:\Pycharm\GA\data\Period1_Point.shp"
od_period2_path = r"E:\Pycharm\GA\data\Period2_Point.shp"
od_period3_path = r"E:\Pycharm\GA\data\Period3_Point.shp"
def load_od_as_dataframe(od_path):
    cursor = load_data(od_path, ["SHAPE@XY", "weight"])
    points = [(row[0][0], row[0][1], row[1]) for row in cursor]
    return pd.DataFrame(points, columns=["X", "Y", "weight"])

od_points_period1_df = load_od_as_dataframe(od_period1_path)
od_points_period2_df = load_od_as_dataframe(od_period2_path)
od_points_period3_df = load_od_as_dataframe(od_period3_path)

# 网络数据集
network_dataset_path = r"E:\Pycharm\GA\data\RoadNetwork.gdb\RoadNetwork\RoadNetwork_ND"
available_attributes = list_network_attributes(network_dataset_path)
if "TravelTime" not in available_attributes:
    raise ValueError(f"TravelTime 属性不存在，可用属性为: {available_attributes}")

############################################
# 预计算公交站点之间的旅行时间矩阵（基于网络分析）
############################################
def compute_travel_time_matrix(bus_stop_path, network_dataset_path):
    od_layer_name = "ODMatrix"
    result_object = arcpy.na.MakeODCostMatrixLayer(network_dataset_path, od_layer_name, "TravelTime")
    od_layer = result_object.getOutput(0)
    na_classes = arcpy.na.GetNAClassNames(od_layer)
    origins_layer = na_classes["Origins"]
    destinations_layer = na_classes["Destinations"]
    origin_field_mappings = arcpy.na.NAClassFieldMappings(od_layer, origins_layer)
    origin_field_mappings["Name"].mappedFieldName = "FID"
    arcpy.na.AddLocations(od_layer, origins_layer, bus_stop_path, origin_field_mappings)
    dest_field_mappings = arcpy.na.NAClassFieldMappings(od_layer, destinations_layer)
    dest_field_mappings["Name"].mappedFieldName = "FID"
    arcpy.na.AddLocations(od_layer, destinations_layer, bus_stop_path, dest_field_mappings)
    arcpy.na.Solve(od_layer)
    origins_array = arcpy.da.TableToNumPyArray(f"{od_layer}/{origins_layer}", ["OID@", "Name"])
    origin_map = {row["OID@"]: row["Name"] for row in origins_array}
    dest_array = arcpy.da.TableToNumPyArray(f"{od_layer}/{destinations_layer}", ["OID@", "Name"])
    dest_map = {row["OID@"]: row["Name"] for row in dest_array}
    all_fids = sorted(bus_stops_df["FID"].unique())
    fid_to_index = {fid: idx for idx, fid in enumerate(all_fids)}
    travel_times = np.full((len(all_fids), len(all_fids)), float('inf'))
    od_lines_table = f"{od_layer}/{na_classes['ODLines']}"
    lines_array = arcpy.da.TableToNumPyArray(od_lines_table, ["OriginID", "DestinationID", "Total_TravelTime"])
    for line in lines_array:
        origin_oid = line["OriginID"]
        dest_oid = line["DestinationID"]
        total_time = line["Total_TravelTime"]
        if total_time is not None:
            fid1 = int(origin_map[origin_oid])
            fid2 = int(dest_map[dest_oid])
            if fid1 in fid_to_index and fid2 in fid_to_index:
                idx1 = fid_to_index[fid1]
                idx2 = fid_to_index[fid2]
                travel_times[idx1][idx2] = total_time
    return travel_times, fid_to_index, all_fids

travel_times, fid_to_index, all_fids = compute_travel_time_matrix(bus_stop_path, network_dataset_path)
fid_locations = {row["FID"]: (row["X"], row["Y"]) for _, row in bus_stops_df.iterrows()}

# 全局归一化因子：travel_times中所有有限值的最大值
max_travel_time = np.max(travel_times[np.isfinite(travel_times)])
print(f"Max travel time for normalization: {max_travel_time}")

############################################
# 预计算 OD 点到公交站点的网络成本矩阵（基于网络分析）
############################################
def compute_od_bus_matrix(od_shp_path, bus_stop_shp_path, network_dataset_path):
    network_sr = arcpy.Describe(network_dataset_path).spatialReference
    od_desc = arcpy.Describe(od_shp_path)
    if od_desc.shapeType != "Point":
        raise ValueError(f"{od_shp_path} 必须是点要素，但当前类型为 {od_desc.shapeType}")
    if od_desc.spatialReference.name == "Unknown" or od_desc.spatialReference.factoryCode != network_sr.factoryCode:
        temp_od_projected = os.path.join(arcpy.env.workspace, f"temp_{os.path.basename(od_shp_path)}_projected.shp")
        arcpy.Project_management(od_shp_path, temp_od_projected, network_sr)
        od_shp_for_layer = temp_od_projected
    else:
        od_shp_for_layer = od_shp_path
    od_layer_name = "ODMatrix_OD"
    result = arcpy.na.MakeODCostMatrixLayer(network_dataset_path, od_layer_name, "TravelTime")
    od_layer = result.getOutput(0)
    na_classes = arcpy.na.GetNAClassNames(od_layer)
    origins_layer = na_classes["Origins"]
    destinations_layer = na_classes["Destinations"]
    arcpy.na.AddLocations(od_layer, origins_layer, od_shp_for_layer)
    dest_field_mappings = arcpy.na.NAClassFieldMappings(od_layer, destinations_layer)
    dest_field_mappings["Name"].mappedFieldName = "FID"
    arcpy.na.AddLocations(od_layer, destinations_layer, bus_stop_path, dest_field_mappings)
    arcpy.na.Solve(od_layer)
    dest_array = arcpy.da.TableToNumPyArray(f"{od_layer}/{destinations_layer}", ["OID@", "Name"])
    dest_map = {row["OID@"]: row["Name"] for row in dest_array}
    od_bus_matrix = {}
    od_lines_table = f"{od_layer}/{na_classes['ODLines']}"
    lines_array = arcpy.da.TableToNumPyArray(od_lines_table, ["OriginID", "DestinationID", "Total_TravelTime"])
    for line in lines_array:
        origin_oid = line["OriginID"]
        travel_cost = line["Total_TravelTime"]
        if travel_cost is None:
            continue
        dest_oid = line["DestinationID"]
        bus_stop_fid = int(dest_map[dest_oid])
        if origin_oid not in od_bus_matrix:
            od_bus_matrix[origin_oid] = {}
        od_bus_matrix[origin_oid][bus_stop_fid] = travel_cost
    return od_bus_matrix

def compute_od_weights(od_shp_path):
    weights = {}
    with arcpy.da.SearchCursor(od_shp_path, ["OID@", "weight"]) as cursor:
        for row in cursor:
            weights[row[0]] = row[1]
    return weights

od_bus_matrix_period1 = compute_od_bus_matrix(od_period1_path, bus_stop_path, network_dataset_path)
od_bus_matrix_period2 = compute_od_bus_matrix(od_period2_path, bus_stop_path, network_dataset_path)
od_bus_matrix_period3 = compute_od_bus_matrix(od_period3_path, bus_stop_path, network_dataset_path)
od_weights_period1 = compute_od_weights(od_period1_path)
od_weights_period2 = compute_od_weights(od_period2_path)
od_weights_period3 = compute_od_weights(od_period3_path)

############################################
# 定义目标函数：返回多个目标值
# 目标1：网络覆盖得分（放大高权重区域）： f1 = -total_coverage
# 目标2：站点间旅行时间归一化总和： f2 = norm_dist_score
# 目标3：地理覆盖率： f3 = -geo_cov，其中覆盖判断在WGS84下调整阈值为约0.005度（约500米）
############################################
def evaluate_objectives(individual, od_bus_matrix_p1, od_bus_matrix_p2, od_bus_matrix_p3,
                        od_weights_p1, od_weights_p2, od_weights_p3,
                        districts_df, travel_times, fid_to_index, weight_exponent=2,
                        geo_threshold=0.005):
    # 目标1：网络覆盖得分，采用 weight 的幂次放大，以鼓励覆盖高需求区域
    def calc_network_coverage(od_bus_matrix, od_weights):
        cov = 0
        for od_oid, cost_dict in od_bus_matrix.items():
            weight = od_weights.get(od_oid, 0)
            weight_mod = weight ** weight_exponent  # 权重放大
            min_cost = float('inf')
            for bus_fid in individual.stops:
                if bus_fid in cost_dict:
                    min_cost = min(min_cost, cost_dict[bus_fid])
            if min_cost == float('inf'):
                min_cost = 1e6
            cov += weight_mod / (1 + min_cost)
        return cov
    total_coverage = (calc_network_coverage(od_bus_matrix_p1, od_weights_p1) +
                      calc_network_coverage(od_bus_matrix_p2, od_weights_p2) +
                      calc_network_coverage(od_bus_matrix_p3, od_weights_p3))
    f1 = -total_coverage

    # 目标2：站点间旅行时间总和归一化
    dist_score = 0
    for i in range(len(individual.stops)-1):
        fid_a = individual.stops[i]
        fid_b = individual.stops[i+1]
        idx_a = fid_to_index[fid_a]
        idx_b = fid_to_index[fid_b]
        cost = travel_times[idx_a][idx_b]
        if math.isinf(cost):
            cost = 1e6
        dist_score += cost
    norm_dist_score = dist_score / ((len(individual.stops)-1) * max_travel_time)
    f2 = norm_dist_score

    # 目标3：地理覆盖率：判断每个街区是否有候选站点位于规定阈值内（WGS84下取0.005度约等于500米）
    geo_cov = 0
    for _, row in districts_df.iterrows():
        for fid in individual.stops:
            stop_coords = fid_locations[fid]
            d = math.sqrt((row["X"] - stop_coords[0])**2 + (row["Y"] - stop_coords[1])**2)
            if d < geo_threshold:
                geo_cov += 1
                break
    f3 = -geo_cov

    return (f1, f2, f3)

############################################
# 个体类：存储决策变量及目标值
############################################
class BusStopIndividual:
    def __init__(self, stops, travel_times, fid_to_index):
        self.stops = stops
        self.travel_times = travel_times
        self.fid_to_index = fid_to_index
        self.objectives = self.evaluate_objectives(districts_df)
    def evaluate_objectives(self, districts_df):
        return evaluate_objectives(self,
                                   od_bus_matrix_period1, od_bus_matrix_period2, od_bus_matrix_period3,
                                   od_weights_period1, od_weights_period2, od_weights_period3,
                                   districts_df, self.travel_times, self.fid_to_index,
                                   weight_exponent=2, geo_threshold=0.005)

############################################
# NSGA-II 的快速非支配排序与拥挤距离计算
############################################
def dominates(indiv1, indiv2):
    objs1 = indiv1.objectives
    objs2 = indiv2.objectives
    return all(a <= b for a, b in zip(objs1, objs2)) and any(a < b for a, b in zip(objs1, objs2))

def fast_non_dominated_sort(population):
    S = {i: [] for i in range(len(population))}
    n = [0] * len(population)
    rank = [0] * len(population)
    fronts = [[]]
    for p in range(len(population)):
        for q in range(len(population)):
            if p == q:
                continue
            if dominates(population[p], population[q]):
                S[p].append(q)
            elif dominates(population[q], population[p]):
                n[p] += 1
        if n[p] == 0:
            rank[p] = 0
            fronts[0].append(p)
    i = 0
    while fronts[i]:
        Q = []
        for p in fronts[i]:
            for q in S[p]:
                n[q] -= 1
                if n[q] == 0:
                    rank[q] = i+1
                    Q.append(q)
        i += 1
        fronts.append(Q)
    fronts.pop()
    return fronts

def crowding_distance_assignment(front, population):
    distance = [0] * len(front)
    num_obj = len(population[0].objectives)
    for m in range(num_obj):
        front_objs = [population[i].objectives[m] for i in front]
        sorted_idx = sorted(range(len(front_objs)), key=lambda k: front_objs[k])
        distance[sorted_idx[0]] = distance[sorted_idx[-1]] = float('inf')
        m_max = max(front_objs)
        m_min = min(front_objs)
        if m_max == m_min:
            continue
        for j in range(1, len(front_objs)-1):
            distance[sorted_idx[j]] += (front_objs[sorted_idx[j+1]] - front_objs[sorted_idx[j-1]])/(m_max-m_min)
    return distance

def tournament_selection(population, tournament_size):
    participants = random.sample(population, tournament_size)
    fronts = fast_non_dominated_sort(population)
    rank_dict = {}
    for r, front in enumerate(fronts):
        for i in front:
            rank_dict[i] = r
    indices = [population.index(ind) for ind in participants]
    front_indices = list(set(indices))
    cd = crowding_distance_assignment(front_indices, population)
    best = participants[0]
    for ind in participants:
        i1 = population.index(best)
        i2 = population.index(ind)
        if rank_dict[i2] < rank_dict[i1]:
            best = ind
        elif rank_dict[i2] == rank_dict[i1] and cd[front_indices.index(i2)] > cd[front_indices.index(i1)]:
            best = ind
    return best

############################################
# 初始化种群
############################################
def initialize_population(size, stop_count, bus_stops_df, travel_times, fid_to_index):
    population = []
    fids = bus_stops_df["FID"].values
    for _ in range(size):
        stops = random.sample(list(fids), stop_count)
        population.append(BusStopIndividual(stops, travel_times, fid_to_index))
    return population

############################################
# 交叉与变异算子（保持原逻辑）
############################################
def crossover(parent1_stops, parent2_stops):
    size = len(parent1_stops)
    cp1 = random.randint(0, size - 2)
    cp2 = random.randint(cp1 + 1, size - 1)
    child1 = [None] * size
    child2 = [None] * size
    child1[cp1:cp2+1] = parent1_stops[cp1:cp2+1]
    child2[cp1:cp2+1] = parent2_stops[cp1:cp2+1]
    def fill_child(child, parent, start, end):
        current_idx = (end+1)%size
        parent_idx = (end+1)%size
        while None in child:
            if parent[parent_idx] not in child:
                child[current_idx] = parent[parent_idx]
                current_idx = (current_idx+1)%size
            parent_idx = (parent_idx+1)%size
    fill_child(child1, parent2_stops, cp1, cp2)
    fill_child(child2, parent1_stops, cp1, cp2)
    return child1, child2

def mutate(individual_stops, mutation_rate, available_stops):
    new_stops = individual_stops.copy()
    for i in range(len(new_stops)):
        if random.random() < mutation_rate:
            poss = list(set(available_stops) - set(new_stops))
            if poss:
                new_stops[i] = random.choice(poss)
    return new_stops

############################################
# 定义生成后代的函数，用于并发运行
############################################
def generate_offspring(population, tournament_size, available_stops, travel_times, fid_to_index, crossover_rate, mutation_rate):
    parent1 = tournament_selection(population, tournament_size)
    parent2 = tournament_selection(population, tournament_size)
    if random.random() < crossover_rate:
        child_stops1, child_stops2 = crossover(parent1.stops, parent2.stops)
    else:
        child_stops1, child_stops2 = parent1.stops.copy(), parent2.stops.copy()
    child_stops1 = mutate(child_stops1, mutation_rate, available_stops)
    child_stops2 = mutate(child_stops2, mutation_rate, available_stops)
    return (BusStopIndividual(child_stops1, travel_times, fid_to_index),
            BusStopIndividual(child_stops2, travel_times, fid_to_index))

############################################
# 多目标 NSGA-II 算法（带调试信息）
############################################
def nsga2(population, num_generations, tournament_size, available_stops, travel_times, fid_to_index, workers=8,
          crossover_rate=0.8, mutation_rate=0.05):
    for gen in range(num_generations):
        start_time = time.time()
        offspring = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            num_offspring = len(population) // 2
            for _ in range(num_offspring):
                futures.append(executor.submit(generate_offspring, population, tournament_size,
                                               available_stops, travel_times, fid_to_index,
                                               crossover_rate, mutation_rate))
            for future in as_completed(futures):
                try:
                    child1, child2 = future.result()
                    offspring.extend([child1, child2])
                except Exception as e:
                    print("Error generating offspring:", e)
        combined = population + offspring
        fronts = fast_non_dominated_sort(combined)
        new_population = []
        for front in fronts:
            if len(new_population) + len(front) <= len(population):
                for i in front:
                    new_population.append(combined[i])
            else:
                cd = crowding_distance_assignment(front, combined)
                front_indices = list(front)
                sorted_front = sorted(front_indices, key=lambda i: cd[front.index(i)], reverse=True)
                needed = len(population) - len(new_population)
                for i in sorted_front[:needed]:
                    new_population.append(combined[i])
                break
        population = new_population
        end_time = time.time()
        avg_f1 = sum(ind.objectives[0] for ind in population) / len(population)
        avg_f2 = sum(ind.objectives[1] for ind in population) / len(population)
        avg_f3 = sum(ind.objectives[2] for ind in population) / len(population)
        best = min(population, key=lambda ind: ind.objectives[0])
        print(f"Generation {gen}: Best objectives = {best.objectives}, " +
              f"Avg objectives = ({avg_f1:.4f}, {avg_f2:.4f}, {avg_f3:.4f}), " +
              f"Time = {end_time - start_time:.2f}s")
    fronts = fast_non_dominated_sort(population)
    pareto_front = [population[i] for i in fronts[0]]
    return pareto_front

############################################
# 主程序入口
############################################
stop_count = 20  # 固定站点数
population_size = 70
num_generations = 500
tournament_size = 4
available_stops = list(bus_stops_df["FID"].values)

population = initialize_population(population_size, stop_count, bus_stops_df, travel_times, fid_to_index)
pareto_front = nsga2(population, num_generations, tournament_size, available_stops, travel_times, fid_to_index,
                     workers=8, crossover_rate=0.8, mutation_rate=0.05)

# 选择 Pareto 前沿中一个个体作示例输出
best = pareto_front[0]
best_stops_coords = [fid_locations[fid] for fid in best.stops]
print("Pareto前沿个体中的一个候选解的站点位置:")
print(best_stops_coords)

############################################
# 输出结果为 Shapefile
############################################
output_shp_path = r"E:\Pycharm\GA\data\Best_Bus_Stops.shp"
output_folder = os.path.dirname(output_shp_path)
output_name = os.path.basename(output_shp_path)
spatial_ref = arcpy.SpatialReference(4326)
arcpy.CreateFeatureclass_management(output_folder, output_name, "POINT", spatial_reference=spatial_ref)
arcpy.AddField_management(output_shp_path, "StopID", "LONG")
with arcpy.da.InsertCursor(output_shp_path, ["SHAPE@XY", "StopID"]) as cursor:
    for fid in best.stops:
        coords = fid_locations[fid]
        cursor.insertRow([(coords[0], coords[1]), fid])
print(f"候选站点已保存为 shapefile: {output_shp_path}")
