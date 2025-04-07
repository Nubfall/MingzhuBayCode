import math
import time
import warnings
import geopandas as gpd
from shapely.ops import unary_union
from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon


def filter_geom(geom):
    """过滤非多边形的几何对象，并确保几何对象有效"""
    if geom.is_empty or not geom.is_valid:
        return None

    if geom.geom_type == 'GeometryCollection':
        polygons = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        return MultiPolygon(polygons) if len(polygons) > 1 else polygons[0] if polygons else None

    return geom if isinstance(geom, (Polygon, MultiPolygon)) else None


def meter2degree(meter):
    return meter / (2 * math.pi * 6371004) * 360


def getWidth(x):
    road_width_map = {
        'motorway': 24, 'trunk': 24, 'trunk_link': 24, 'motorway_link': 24,
        'primary': 20, 'primary_link': 20,
        'secondary': 16, 'secondary_link': 16,
        'tertiary': 12, 'tertiary_link': 12,
        'residential': 8, 'unclassified': 8,
        'construction':8
    }
    return road_width_map.get(x.get("fclass"), 4)


def getThreshold(x):
    return {5: 16, 4: 22, 3: 28, 2: 35, 1: 42}.get(x, 0)


def getMinarea(x):
    return {5: 100, 4: 100, 3: 1000, 2: 10000, 1: 10000}.get(x, 0)


def getZ0(m_level, m_region, m_road):
    m_road.loc[:, 'load_width'] = m_road.apply(getWidth, axis=1)

    if m_level <= 5:
        m_road = m_road[m_road['load_width'] == (7 - m_level) * 4]

    if m_road.empty:
        return None

    buffer = m_road.buffer(distance=meter2degree(m_road['load_width']), resolution=8)
    buffer = buffer.apply(filter_geom).dropna().buffer(0)

    m_region = gpd.GeoDataFrame(geometry=gpd.GeoSeries(unary_union(m_region['geometry'].buffer(0, resolution=8))))

    try:
        un = unary_union(buffer)
    except Exception as e:
        print(f"ERROR: unary_union 失败: {str(e)}")
        return None

    buffer = gpd.GeoDataFrame(geometry=gpd.GeoSeries(un))
    buffer.crs = "EPSG:4326"

    result = gpd.overlay(df1=m_region, df2=buffer, how='difference', keep_geom_type=False).explode()
    return result


def exportSHP(m_geoDataFrame, m_crs, m_name):
    try:
        m_geoDataFrame.crs = m_crs
        m_geoDataFrame.to_file(f'output/{m_name}.shp', driver='ESRI Shapefile', encoding='GBK')
        print(f"成功保存 {m_name}.shp")
    except Exception as e:
        print(f"ERROR: 保存 {m_name}.shp 失败: {str(e)}")


def wipe_hole_str(x):
    wkt_str = wkt.dumps(x)
    return wkt_str if '), (' not in wkt_str else wkt_str.split('), (')[0] + '))'


def wipe_hole(x):
    try:
        x['no_hole'] = x['geometry'].apply(wipe_hole_str)
        x['geometry'] = gpd.GeoSeries.from_wkt(x['no_hole'])
    except Exception as e:
        print(f"ERROR: wipe_hole 失败: {str(e)}")
    return x


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    epsg = "EPSG:4326"
    regionname = "add"

    region = gpd.read_file(regionname + 'admin.shp', encoding='GBK')
    road = gpd.read_file(regionname + 'road.shp', encoding='GBK')

    for index, row in region.iterrows():
        try:
            countryname, cityname = row['NAME_0'], row['Name']
            subRegion = gpd.GeoDataFrame(geometry=gpd.GeoSeries(row['geometry']))
            subRegion.crs = epsg
            exportSHP(subRegion, epsg, cityname + 'admin')

            subRoad = gpd.overlay(road, subRegion, 'intersection')
            exportSHP(subRoad, epsg, cityname + 'road')

            for i in range(1, 6):
                z0 = getZ0(i, subRegion, subRoad)
                if z0 is None:
                    continue

                z0.crs = epsg
                z0['area'] = z0['geometry'].area * 9101160000.085981
                z1 = z0[z0['area'] >= getMinarea(i)]
                subRegion = z1

                try:
                    z2 = z1.buffer(distance=meter2degree(getThreshold(i)), resolution=8)
                    z3 = z2.buffer(distance=-meter2degree(getThreshold(i)), resolution=8)
                    z3 = gpd.GeoDataFrame(geometry=gpd.GeoSeries(unary_union(z3))).explode()
                    z3.crs = epsg
                except Exception as e:
                    print(f"ERROR: 缓冲区处理失败: {str(e)}")
                    continue

                try:
                    z3 = wipe_hole(z3)
                    z3 = gpd.GeoDataFrame(geometry=gpd.GeoSeries(unary_union(z3.buffer(0, resolution=8)))).explode()
                except Exception as e:
                    print(f"ERROR: 孔洞处理失败: {str(e)}")

                exportSHP(z3, epsg, f"{countryname}_{cityname}_Level{i}")
        except Exception as e:
            with open('log.txt', 'a') as errorFile:
                errorFile.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}|{countryname}_{cityname}|{str(e)}\n")
