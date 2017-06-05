
package gg.boosted.riotapi.dtos.match;

import java.util.List;

public class Match  {

	public long gameCreation;
	public long gameDuration;
	public long gameId;
	public String gameMode;
	public String gameType;
	public String gameVersion;
	public int mapId;
	public List<ParticipantIdentity> participantIdentities;
	public List<Participant> participants;
	public String platformId;
	public int queueId;
	public int seasonId;
	public List<TeamStats> teams;

}