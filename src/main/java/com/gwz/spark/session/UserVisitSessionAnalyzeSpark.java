package com.gwz.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.gwz.constant.Constants;
import com.gwz.dao.ITaskDAO;
import com.gwz.dao.factory.DAOFactory;
import com.gwz.domain.Task;
import com.gwz.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

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

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.cache();

        //获取通过筛选条件的session的访问明细数据RDD
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

        //随机抽取索引
        randomExtractSession(sc, task.getTaskid(),
                filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

    }

    /**
     * 随机抽取session
     * @param sc
     * @param taskid
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2detailRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2detailRDD) {

        // 获取<yyyy-MM-dd,<hour,aggrInfo>>格式的RDD
        JavaPairRDD<String, Tuple2<String, String>> date2hour_sessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];

                return new Tuple2<String, Tuple2<String, String>>(date,new Tuple2<String, String>(hour,aggrInfo));
            }
        });
        //String:date   某天,
        // Object:某天对应的session数量
        Map<String, Object> countMap = date2hour_sessionidRDD.countByKey();

        /**
         * 计算每天每小时的session数量
         * <String, Map<String, Long>>
         *     String：date  某天
         *          String：这天对应的某一小时
         *          Long：这一小时对应的session数量
         */
        JavaPairRDD<String, Map<String, Long>> date2Hour_Count = date2hour_sessionidRDD
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Map<String, Long>>() {
            @Override
            public Tuple2<String, Map<String, Long>> call(Tuple2<String, Iterable<Tuple2<String, String>>> tuple) throws Exception {
                String date = tuple._1;
                Map<String, Long> hourCountMap = new HashMap<String, Long>();
                Iterator<Tuple2<String, String>> iterator = tuple._2.iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, String> next = iterator.next();
                    String hour = next._1;
                    Long count = hourCountMap.get(hour);
                    if (count == 0L) {
                        hourCountMap.put(hour, 1L);
                    } else {
                        Long newCount = count + 1L;
                        hourCountMap.put(hour, newCount);
                    }
                }
                return new Tuple2<String, Map<String, Long>>(date, hourCountMap);
            }
        });


        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / countMap.size();



    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<String, Row>(tuple._1,tuple._2._2);
            }
        });
        return sessionid2detailRDD;
    }

    /**
     * 过滤session数据并进行聚合统计
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {

        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;

                // 接着，依次按照筛选条件进行过滤
                // 按照年龄范围进行过滤（startAge、endAge）
                if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                        parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }

                // 按照职业范围进行过滤（professionals）
                // 互联网,IT,软件
                // 互联网
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                        parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }

                // 按照城市范围进行过滤（cities）
                // 北京,上海,广州,深圳
                // 成都
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                        parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                // 按照性别进行过滤
                // 男/女
                // 男，女
                if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                        parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                        parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类id进行过滤
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                // 进行相应的累加计数

                // 主要走到这一步，那么就是需要计数的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);

                return true;

            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });

        return filteredSessionid2AggrInfoRDD;

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

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String session_id = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String profession = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + profession + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<String, String>(session_id,fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;
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
