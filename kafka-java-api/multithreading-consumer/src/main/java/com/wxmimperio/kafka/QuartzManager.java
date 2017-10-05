package com.wxmimperio.kafka;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by weiximing.imperio on 2016/8/3.
 */
public class QuartzManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuartzManager.class);
    private final String jobGroupName;
    private final String triggerGroupName;
    private Scheduler scheduler;
    private final static SchedulerFactory sf = new StdSchedulerFactory();
    private final static Map<String, QuartzManager> instanceMap = new ConcurrentHashMap<String, QuartzManager>();
    private final static Map<String, Scheduler> schedulerMap = new ConcurrentHashMap<String, Scheduler>();

    private QuartzManager(String jobGroupName, String triggerGroupName) {
        this.jobGroupName = jobGroupName;
        this.triggerGroupName = triggerGroupName;
    }

    /**
     * @param jobGroupName
     * @param triggerGroupName
     * @return
     */
    public synchronized static QuartzManager getInstance(String jobGroupName, String triggerGroupName) {
        String instanceKey = jobGroupName + "_" + triggerGroupName;
        if (instanceMap.containsKey(instanceKey)) {
            return instanceMap.get(instanceKey);
        }
        QuartzManager quartzUtil = new QuartzManager(jobGroupName, triggerGroupName);
        instanceMap.put(instanceKey, quartzUtil);
        return quartzUtil;
    }

    /**
     * @return
     * @throws SchedulerException
     */
    private Scheduler getScheduler() throws SchedulerException {
        if (scheduler == null) {
            scheduler = sf.getScheduler();
        }
        return scheduler;
    }

    /**
     * @param jobName
     * @param triggerName
     * @param cls
     * @param cronSchedule
     * @param jobBindData
     * @throws SchedulerException
     */
    public void addJob(String jobName, String triggerName, Class cls,
                       String cronSchedule, Map<String, Object> jobBindData) throws SchedulerException {

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, 2);
        Date runTime = DateBuilder.evenSecondDate(cal.getTime());

        JobDetail jobDetail = JobBuilder.newJob(cls).withIdentity(jobName, jobGroupName).build();

        if (CronExpression.isValidExpression(cronSchedule)) {
            //bind data
            if (jobBindData != null) {
                for (Map.Entry<String, Object> entry : jobBindData.entrySet()) {
                    jobDetail.getJobDataMap().put(entry.getKey(), entry.getValue());
                }
            }

            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(triggerName, triggerGroupName)
                    .startAt(runTime)
                    .withSchedule(
                            CronScheduleBuilder.cronSchedule(cronSchedule)
                    ).build();

            Scheduler scheduler = getScheduler();
            scheduler.scheduleJob(jobDetail, trigger);

            String schedulerKey = jobGroupName + "<" + jobName + ">" + "-" + triggerGroupName + "<" + triggerName + ">";
            schedulerMap.put(schedulerKey, scheduler);

            if (!scheduler.isShutdown()) {
                scheduler.start();
            }
        } else {
            LOG.error("IllegalArgument cronExpression!");
        }
    }

    /**
     * @param waitForJobsToComplete
     */
    public void shutdown(boolean waitForJobsToComplete) {
        String instanceKey = jobGroupName + "_" + triggerGroupName;
        if (instanceMap.containsKey(instanceKey)) {
            QuartzManager quartzUtil = instanceMap.get(instanceKey);
            if (quartzUtil != null && quartzUtil.scheduler != null) {
                try {
                    quartzUtil.scheduler.shutdown(waitForJobsToComplete);
                } catch (SchedulerException ex) {
                    LOG.error(ex.getLocalizedMessage(), ex);
                }
            }
        }
    }

    public void shutdown() {
        shutdown(false);
    }

    /**
     * @param jobName
     * @param triggerName
     * @return
     */
    public Scheduler isSchedulerExist(String jobName, String triggerName) {
        String schedulerKey = jobGroupName + "<" + jobName + ">" + "-" + triggerGroupName + "<" + triggerName + ">";
        if (schedulerMap.containsKey(schedulerKey)) {
            return schedulerMap.get(schedulerKey);
        } else {
            return null;
        }
    }

    /**
     * @param jobName
     * @param jobGroupName
     * @param triggerName
     * @param triggerGroupName
     */
    public void removeJob(String jobName, String jobGroupName, String triggerName, String triggerGroupName) {
        try {
            TriggerKey tk = TriggerKey.triggerKey(triggerName, triggerGroupName);
            scheduler.pauseTrigger(tk);
            scheduler.unscheduleJob(tk);
            JobKey jobKey = JobKey.jobKey(jobName, jobGroupName);
            scheduler.deleteJob(jobKey);
            LOG.info("delete job => [JobName：" + jobName + " JobGroup：" + jobGroupName + "] ");
        } catch (SchedulerException e) {
            e.printStackTrace();
            LOG.error("delete job => [JobName：" + jobName + " JobGroup：" + jobGroupName + "] ");
        }
    }
}
