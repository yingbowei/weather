//package com.youzidata.weather.dao;
//
//import java.util.List;
//
//import org.springframework.data.jpa.repository.JpaRepository;
//import org.springframework.data.jpa.repository.Query;
//
//import com.youzidata.weather.entity.ViewJsCdStationEntity;
//
///**
// * @author ZhangZhiWu
// * @version 创建时间：2019年1月16日 下午3:36:11
// *
// */
//public interface WeatherDao extends JpaRepository<ViewJsCdStationEntity, String>{
//	@Query(value = "select * from VIEW_JS_CD_STATION", nativeQuery = true)
//	public List<ViewJsCdStationEntity> selectViewJsCdStation();
//}
