package com.demo.ipl.iplvisualizer.config;

import com.demo.ipl.iplvisualizer.model.Match;

import com.demo.ipl.iplvisualizer.model.Team;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {
    //private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final EntityManager entityManager;

    @Autowired
    public JobCompletionNotificationListener(EntityManager entityManager) {
        this.entityManager = entityManager;
    }


    @Override
    @Transactional
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            System.out.println("!!! JOB FINISHED! Time to verify the results");
            Map<String, Team> teamData = new HashMap<>();

            // gets the list of team1 matches from match table convert it into team  for
            // later persistence
            entityManager.createQuery("select m.team1, count(*) " +
                            "from Match m group by m.team1",
                    Object[].class)
                    .getResultList()
                    .stream()
                    .map(e -> new Team((String) e[0], (long) e[1]))
                    .forEach(team -> teamData.put(team.getTeamName(), team));

            // gets the list of team2 matches from match table
            entityManager.createQuery("select m.team2, count(*) from " +
                            "Match m group by m.team2",
                    Object[].class)
                    .getResultList()
                    .stream()
                    .forEach(e -> {
                        Team team = teamData.get((String) e[0]);
                        team.setTotalMatches(team.getTotalMatches() + (long) e[1]);
                    });
            // gets the list of match winner from match table to get no of wins for the particuar
            //team
            entityManager.createQuery("select m.matchWinner, count(*) from Match " +
                    "m group by m.matchWinner", Object[].class)
                    .getResultList()
                    .stream()
                    .forEach(e -> {
                        Team team = teamData.get((String) e[0]);
                        if (team != null) team.setTotalWins((long) e[1]);
                    });

            teamData.values().forEach(team -> entityManager.persist(team));
            teamData.values().forEach(team -> System.out.println(team));
            System.out.println("Loading team data competed");
        }
    }

}
