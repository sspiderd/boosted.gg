/*
 * Copyright 2016 Taylor Caldwell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.rithms.riot.api.endpoints.runes.methods;

import net.rithms.riot.api.ApiConfig;
import net.rithms.riot.api.endpoints.runes.RunesApiMethod;
import net.rithms.riot.api.endpoints.runes.dto.RunePages;
import net.rithms.riot.constant.Platform;

public class GetRunesBySummoner extends RunesApiMethod {

	public GetRunesBySummoner(ApiConfig config, Platform platform, long summonerId) {
		super(config);
		setPlatform(platform);
		setReturnType(RunePages.class);
		setUrlBase(platform.getHost() + "/lol/platform/v3/runes/by-summoner/" + summonerId);
		addApiKeyParameter();
	}
}