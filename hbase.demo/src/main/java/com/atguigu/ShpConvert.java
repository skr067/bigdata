package com.atguigu;

import java.io.File;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import com.lzugis.CommonMethod;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon
public class ShpConvert {
	
	/**
	 * shp转换为Geojson
	 * @return
	 */
	public Map shape2Geojson(String shpPath, String jsonPath){
		Map map = new HashMap();
		
		FeatureJSON fjson = new FeatureJSON();
		
		try{
			StringBuffer sb = new StringBuffer();
			sb.append("{\"type\": \"FeatureCollection\",\"features\": ");
			
			File file = new File(shpPath);
    		ShapefileDataStore shpDataStore = null;
    		
        	shpDataStore = new ShapefileDataStore(file.toURL());
            //设置编码
            Charset charset = Charset.forName("GBK");
            shpDataStore.setCharset(charset);
            String typeName = shpDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = null;
            featureSource =  shpDataStore.getFeatureSource (typeName);
            SimpleFeatureCollection result = featureSource.getFeatures();
            SimpleFeatureIterator itertor = result.features();
            JSONArray array = new JSONArray();
            while (itertor.hasNext())
            {
                SimpleFeature feature = itertor.next();
                StringWriter writer = new StringWriter();
                fjson.writeFeature(feature, writer);
                JSONObject json = new JSONObject(writer.toString());
                array.put(json);
            }
            itertor.close();
            sb.append(array.toString());
            sb.append("}");
            
            //写入文件
            cm.append2File(jsonPath, sb.toString());
            
			map.put("status", "success");
			map.put("message", sb.toString());
		}
		catch(Exception e){
			map.put("status", "failure");
			map.put("message", e.getMessage());
			e.printStackTrace();
			
		}
		return map;
	}

}
