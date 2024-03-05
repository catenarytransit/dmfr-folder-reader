use std::fs;
use dmfr::*;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OperatorPairInfo {
    pub operator_id: String,
    pub gtfs_agency_id: Option<String>,
}

pub type FeedId = String;
pub type OperatorId = String;

pub struct ReturnDmfrAnalysis {
    pub feedhashmap: HashMap<FeedId, dmfr::Feed>,
    pub operatorhashmap: HashMap<OperatorId, dmfr::Operator>,
    pub operator_to_feed_hashmap: HashMap<OperatorId, Vec<dmfr::OperatorAssociatedFeedsItem>>,
    pub feed_to_operator_pairs_hashmap: HashMap<FeedId, Vec<OperatorPairInfo>>
}

pub fn read_folders(path: &str) -> ReturnDmfrAnalysis {
    let entries = fs::read_dir(path).expect("Transitland atlas missing");

    let mut feedhashmap: HashMap<FeedId, dmfr::Feed> = HashMap::new();
    let mut operatorhashmap: HashMap<OperatorId, dmfr::Operator> = HashMap::new();
    let mut operator_to_feed_hashmap: HashMap<OperatorId, Vec<dmfr::OperatorAssociatedFeedsItem>> =
        HashMap::new();
    let mut feed_to_operator_hashmap: HashMap<FeedId, Vec<OperatorId>> = HashMap::new();
    let mut feed_to_operator_pairs_hashmap: HashMap<FeedId, Vec<OperatorPairInfo>> = HashMap::new();
    
    for entry in entries {
        if let Ok(entry) = entry {
            if let Some(file_name) = entry.file_name().to_str() {
                //println!("{}", file_name);
                let contents = fs::read_to_string(format!("{}/{}", path, file_name));
                if contents.is_err() {
                    eprintln!("Error Reading File: {}", contents.unwrap_err());
                    continue;
                }
                let dmfrinfo: Result<dmfr::DistributedMobilityFeedRegistry, SerdeError> =
                    serde_json::from_str(&contents.unwrap());
                match dmfrinfo {
                    Ok(dmfrinfo) => {
                        dmfrinfo.feeds.iter().for_each(|feed| {
                            //for every operator in the file
                            for operator in feed.operators.to_owned().into_iter() {
                                if feed_to_operator_pairs_hashmap.contains_key(&feed.id) {
                                    let mut existing_operator_pairs =
                                        feed_to_operator_pairs_hashmap
                                            .get(&feed.id)
                                            .unwrap()
                                            .to_owned();
                                    existing_operator_pairs.push(OperatorPairInfo {
                                        operator_id: operator.onestop_id.to_owned(),
                                        gtfs_agency_id: None,
                                    });
                                    feed_to_operator_pairs_hashmap
                                        .insert(feed.id.to_owned(), existing_operator_pairs);
                                } else {
                                    feed_to_operator_pairs_hashmap.insert(
                                        feed.id.to_owned(),
                                        vec![OperatorPairInfo {
                                            operator_id: operator.onestop_id.to_owned(),
                                            gtfs_agency_id: None,
                                        }],
                                    );
                                }

                                feed_to_operator_hashmap
                                    .entry(feed.id.to_owned())
                                    .and_modify(|value| {
                                        value.push(operator.onestop_id.to_owned())
                                    })
                                    .or_insert(vec![operator.onestop_id.to_owned()]);

                                operator_to_feed_hashmap
                                    .entry(operator.onestop_id)
                                    .or_insert(vec![dmfr::OperatorAssociatedFeedsItem {
                                        feed_onestop_id: Some(feed.id.to_owned()),
                                        gtfs_agency_id: None,
                                    }]);
                            }
                            //println!("Feed {}: {:#?}", feed.id.to_owned(), feed);
                            feedhashmap
                                .entry(feed.id.to_owned())
                                .or_insert(feed.to_owned());

                            for operator in feed.operators.iter() {
                                operatorhashmap
                                    .insert(operator.onestop_id.to_owned(), operator.to_owned());
                                if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {
                                    //combine the feeds for this operator together
                                    let mut existing_associated_feeds = operator_to_feed_hashmap
                                        .get(&operator.onestop_id)
                                        .unwrap()
                                        .to_owned();
                                    let existing_feed_ids = operator_to_feed_hashmap
                                        .get(&operator.onestop_id)
                                        .unwrap()
                                        .iter()
                                        .map(|associated_feed| {
                                            associated_feed.feed_onestop_id.to_owned().unwrap()
                                        })
                                        .collect::<Vec<String>>();
                                    operator
                                        .associated_feeds
                                        .iter()
                                        .for_each(|associated_feed| {
                                            if !existing_feed_ids.contains(
                                                &associated_feed
                                                    .feed_onestop_id
                                                    .to_owned()
                                                    .unwrap_or_else(|| feed.id.to_owned()),
                                            ) {
                                                existing_associated_feeds
                                                    .push(associated_feed.to_owned());
                                            }
                                        });
                                    operator_to_feed_hashmap.insert(
                                        operator.onestop_id.to_owned(),
                                        existing_associated_feeds,
                                    );
                                } else {
                                    operator_to_feed_hashmap.insert(
                                        operator.onestop_id.to_owned(),
                                        operator.associated_feeds.to_owned(),
                                    );
                                }
                            }
                        });
                        dmfrinfo.operators.iter().for_each(|operator| {
                            operatorhashmap
                                .insert(operator.onestop_id.to_owned(), operator.to_owned());
                            for feed in operator.associated_feeds.iter() {
                                if feed.feed_onestop_id.is_some() {
                                    feed_to_operator_pairs_hashmap
                                        .entry(feed.feed_onestop_id.as_ref().unwrap().to_owned())
                                        .and_modify(|existing_operator_pairs| {
                                            existing_operator_pairs.push(OperatorPairInfo {
                                                operator_id: operator.onestop_id.to_owned(),
                                                gtfs_agency_id: feed.gtfs_agency_id.to_owned(),
                                            });
                                        })
                                        .or_insert(vec![OperatorPairInfo {
                                            operator_id: operator.onestop_id.to_owned(),
                                            gtfs_agency_id: feed.gtfs_agency_id.to_owned(),
                                        }]);
                                }
                            }
                            if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {
                                //combine the feeds for this operator together
                                let mut existing_associated_feeds = operator_to_feed_hashmap
                                    .get(&operator.onestop_id)
                                    .unwrap()
                                    .to_owned();
                                let existing_feed_ids = operator_to_feed_hashmap
                                    .get(&operator.onestop_id)
                                    .unwrap()
                                    .iter()
                                    .filter(|associated_feed| {
                                        associated_feed.feed_onestop_id.is_some()
                                    })
                                    .map(|associated_feed| {
                                        associated_feed.feed_onestop_id.to_owned().unwrap()
                                    })
                                    .collect::<Vec<String>>();
                                operator
                                    .associated_feeds
                                    .iter()
                                    .for_each(|associated_feed| {
                                        if !existing_feed_ids.contains(
                                            &associated_feed.feed_onestop_id.to_owned().unwrap(),
                                        ) {
                                            existing_associated_feeds
                                                .push(associated_feed.to_owned());
                                        }
                                    });
                                operator_to_feed_hashmap.insert(
                                    operator.onestop_id.to_owned(),
                                    existing_associated_feeds,
                                );
                            } else {
                                operator_to_feed_hashmap.insert(
                                    operator.onestop_id.to_owned(),
                                    operator.associated_feeds.to_owned(),
                                );
                            }
                        });
                    }
                    Err(_) => {}
                }
            }
        }
    }

    ReturnDmfrAnalysis {
        feedhashmap,
        operatorhashmap,
        operator_to_feed_hashmap,
        feed_to_operator_pairs_hashmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let dmfr_result = read_folders("transitland-atlas/feeds");

        assert!(dmfr_result.feedhashmap.len() > 1000);

        println!("{} feeds across {} operators", dmfr_result.feedhashmap.len(), dmfr_result.operatorhashmap.len());

        println!("Operator to feed hashmap length {}", dmfr_result.operator_to_feed_hashmap.len());
        println!("feed_to_operator_pairs_hashmap length {}", dmfr_result.feed_to_operator_pairs_hashmap.len());

        assert!(dmfr_result.feed_to_operator_pairs_hashmap.get("f-bigbluebus~rt").is_some());
    }
}
