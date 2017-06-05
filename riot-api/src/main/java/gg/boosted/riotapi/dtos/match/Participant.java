package gg.boosted.riotapi.dtos.match;

import java.util.List;

public class Participant  {

	public int championId;
	public String highestAchievedSeasonTier;
	public List<Mastery> masteries;
	public int participantId;
	public List<Rune> runes;
	public int spell1Id;
	public int spell2Id;
	public ParticipantStats stats;
	public int teamId;
	public ParticipantTimeline timeline;

}