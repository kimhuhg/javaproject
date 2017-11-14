package com.gwz.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.gwz.constant.Constants;
import com.gwz.dao.ITaskDAO;
import com.gwz.dao.factory.DAOFactory;
import com.gwz.domain.Task;
import com.gwz.util.DateUtils;
import com.gwz.util.ParamUtils;
import com.gwz.util.SparkUtils;
import com.gwz.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.gwz.util.SparkUtils.getSQLContext;

/**
 * Created by gongwenzhou on 2017/11/14.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        SparkUtils.mockData(sc,sqlContext);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //先查询指定任务,再查询出任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskId);
        if(null==task){
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String,Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
        //持久化
        sessionid2actionRDD = sessionid2actionRDD.cache();
        //sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sc, sqlContext, sessionid2actionRDD);



    }

    /**
     * 聚合信息
     * @param sc
     * @param sqlContext
     * @param sessionid2actionRDD   行为数据RDD
     * @return  session粒度的数据<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {
        //根据session_id进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> iterable) throws Exception {
                String session_id = iterable._1;
                Iterator<Row> iterator = iterable._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                Long userid = null;
                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;

                while (iterator.hasNext()){
                    Row row = iterator.next();
                    if(null==userid){
                        userid = row.getLong(1);
                    }

                    String keyWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    //如果搜索的关键字存在并且不在Buffer中则添加(点击的商品类目同理)
                    if(null!=keyWord && !searchKeywordsBuffer.toString().contains(keyWord)){
                        searchKeywordsBuffer.append(keyWord + ",");
                    }
                    if(null!=clickCategoryId && !clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                        clickCategoryIdsBuffer.append(clickCategoryId + ",");
                    }

                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if(startTime==null){
                        startTime = actionTime;
                    }
                    if(endTime==null){
                        endTime = actionTime;
                    }

                    if(actionTime.before(startTime)){
                        startTime = actionTime;
                    }
                    if(actionTime.after(endTime)){
                        endTime = actionTime;
                    }
                    //计算session访问的步长
                    stepLength ++;

                }

                String searchKeyWords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                //计算session的访问时长
                long visitLength = (endTime.getTime()-startTime.getTime()) / 1000;
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + session_id + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);

                return new Tuple2<Long, String>(userid,partAggrInfo);
            }
        });

        //查询所有用户的数据
        String sql = " select * from user_info ";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

//        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.

        return null;

    }

    /**
     * 获取session访问到的对应的action（ROW）
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String, Row>>();
                while(iterator.hasNext()){
                    Row row = iterator.next();
                    String session_id = row.getString(2);
                    list.add(new Tuple2<String, Row>(session_id,row));
                }
                return list;
            }
        });
    }
}
