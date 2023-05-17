package com.demo.ipl.iplvisualizer.controller;

import com.demo.ipl.iplvisualizer.model.Match;
import com.demo.ipl.iplvisualizer.model.Team;
import com.demo.ipl.iplvisualizer.repository.MatchRepository;
import com.demo.ipl.iplvisualizer.repository.TeamRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@RestController
@CrossOrigin
public class IplDashBoardController {
    //sample api ::http://localhost:8080/team/Mumbai%20Indians
    private TeamRepository teamRepository;
    private MatchRepository matchRepository;

    public IplDashBoardController(TeamRepository teamRepository, MatchRepository matchRepository) {
        this.teamRepository = teamRepository;
        this.matchRepository = matchRepository;
    }

    /**
     * Get all teams
     * @return
     */
    @GetMapping("/team")
    public Iterable<Team> getAllTeam() {
        return this.teamRepository.findAll();
    }

    @GetMapping("/team/{teamName}")
    public Team getTeam(@PathVariable String teamName) {
        Team team = this.teamRepository.findByTeamName(teamName);
        team.setMatches(matchRepository.findLatestMatchesbyTeam(teamName,4));

        return team;
    }

    /**
     * Get all matches played by the team in that year.
     * @param teamName
     * @param year
     * @return
     */
    @GetMapping("/team/{teamName}/matches")
    public List<Match> getMatchesForTeam(@PathVariable String teamName, @RequestParam int year) {
        LocalDate startDate = LocalDate.of(year, 1, 1);
        LocalDate endDate = LocalDate.of(year + 1, 1, 1);
        return this.matchRepository.getMatchesByTeamBetweenDates(
                teamName,
                startDate,
                endDate
        );
    }
}
