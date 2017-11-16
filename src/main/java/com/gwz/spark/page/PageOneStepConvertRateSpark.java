package com.gwz.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.gwz.constant.Constants;
import com.gwz.dao.IPageSplitConvertRateDAO;
import com.gwz.dao.ITaskDAO;
import com.gwz.dao.factory.DAOFactory;
import com.gwz.domain.PageSplitConvertRate;
import com.gwz.domain.Task;
import com.gwz.util.DateUtils;
import com.gwz.util.NumberUtils;
import com.gwz.util.ParamUtils;
import com.gwz.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率模块spark作业
 * Created by root on 2017/11/16.
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION);

        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //生成模拟数据
        SparkUtils.mockData(sc,sqlContext);

        //查询任务，获取任务参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null){
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String,Row> sessionId2Action = getSessionId2ActionRDD(actionRDD);
        sessionId2Action = sessionId2Action.cache();
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2Action.groupByKey();


        //分片，获取每个session中符合条件的分片
        JavaPairRDD<String,Integer> pageSplitRDD = generateAndMatchPageSplit(sc,sessionId2ActionsRDD,taskParam);

        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        //获取初始页面的数据
        long startPagePv = getStartPagePv(taskParam, sessionId2ActionsRDD);

        //计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(
                taskParam, pageSplitPvMap, startPagePv);

        // 持久化页面切片转化率
        persistConvertRate(taskId, convertRateMap);


    }

    /**
     * 持久化页面切片转化率
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(Long taskId, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");

        for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);

    }

    /**
     * 计算目标页面流的各个页面切片的转化率
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, long startPagePv) {
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        Map<String, Double> convertRateMap = new HashMap<String, Double>();

        long lastPageSplitPv = 0L;

        for (int i = 1; i < targetPages.length; i++) {
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];

            double convertRate = 0.0;
            Long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            if(i == 1){
                convertRate = NumberUtils.formatDouble(
                        (double)targetPageSplitPv / (double)startPagePv, 2);
            }else{
                convertRate = NumberUtils.formatDouble(
                        (double)targetPageSplitPv / (double)lastPageSplitPv, 2);
            }
            convertRateMap.put(targetPageSplit, convertRate);
            lastPageSplitPv = targetPageSplitPv;
        }

        return convertRateMap;
    }

    /**
     * 获取初始页面的数据
     * @param taskParam
     * @param sessionId2ActionsRDD
     * @return
     */
    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD) {
        final long startPageId = Long.valueOf(ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",")[0]);
        JavaRDD<Long> startPageRDD = sessionId2ActionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            private static final long serialVersionUID = 4729500650297492029L;

            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> list = new ArrayList<Long>();
                Iterator<Row> iterator = tuple._2.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    long pageId = row.getLong(3);
                    if (pageId == startPageId) {
                        list.add(pageId);
                    }
                }
                return list;

            }
        });
        return startPageRDD.count();

    }

    /**
     * 分片，获取每个session中符合条件的分片
     * @param sc
     * @param sessionId2ActionsRDD  <session,row>
     * @param taskParam 任务参数
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD,
            JSONObject taskParam) {
        String pageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> pageFlowBro = sc.broadcast(pageFlow);

        return sessionId2ActionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {
            private static final long serialVersionUID = 3850995607432296020L;

            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();

                List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String, Integer>>();

                String[] pageFlowStand = pageFlowBro.value().split(",");

                //先根据时间对这个session进行排序
                List<Row> rowList = new ArrayList<Row>();
                while (iterator.hasNext()){
                    rowList.add(iterator.next());
                }

                Collections.sort(rowList, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);

                        Date date1 = DateUtils.parseTime(actionTime1);
                        Date date2 = DateUtils.parseTime(actionTime2);

                        return (int)(date1.getTime() - date2.getTime());
                    }
                });


                //页面切片生成，以及页面流的匹配
                Long lastPageId = null;

                for (Row row : rowList) {
                    long currentPageId = row.getLong(3);

                    if(lastPageId == null){
                        lastPageId = currentPageId;
                        continue;
                    }
                    //获得当前的切片
                    String pageSplit = lastPageId + "_" + currentPageId;

                    //判断这个切片是否在用户指定的切片流中
                    for (int i = 1; i < pageFlowStand.length; i++) {
                        String standPartPageFlow = pageFlowStand[i-1] + "_" + pageFlowStand[i];
                        if(pageSplit.equals(standPartPageFlow)){
                            list.add(new Tuple2<String, Integer>(pageSplit,1));
                            break;
                        }
                    }
                    lastPageId = currentPageId;

                }
                return list;
            }
        });

    }


    /**
     * 格式转换<session,row>
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {

        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 4374582629520293786L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
    }

}
