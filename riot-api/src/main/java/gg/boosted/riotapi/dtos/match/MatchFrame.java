

package gg.boosted.riotapi.dtos.match;

import java.util.List;
import java.util.Map;

public class MatchFrame {

	public List<MatchEvent> events;
	public Map<Integer, MatchParticipantFrame> participantFrames;
	public long timestamp;

}