package org.networklibrary.curator.config;

import org.networklibrary.core.config.ConfigManager;

public class CuratorConfigManager extends ConfigManager implements CuratorSettings {

	public CuratorConfigManager(String type, String dictionary) {
		setType(type);
		setDictionaryKey(dictionary);
		
		load(null);
	}

	@Override
	public String getType() {
		return getConfig().getString("type");
	}

	protected void setType(String type){
		getConfig().addProperty("type", type);
	}
	
	private void setDictionaryKey(String dictionary) {
		getConfig().addProperty(DICTIONARY_KEY, dictionary);
	}
}
