use dmfr::*;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OperatorPairInfo {
    pub operator_id: String,
    pub gtfs_agency_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FeedPairInfo {
    pub feed_onestop_id: String,
    pub gtfs_agency_id: Option<String>,
}

pub type FeedId = String;
pub type OperatorId = String;

pub struct ReturnDmfrAnalysis {
    pub feed_hashmap: HashMap<FeedId, dmfr::Feed>,
    pub operator_hashmap: HashMap<OperatorId, dmfr::Operator>,
    pub operator_to_feed_hashmap: HashMap<OperatorId, Vec<FeedPairInfo>>,
    pub feed_to_operator_pairs_hashmap: HashMap<FeedId, Vec<OperatorPairInfo>>,
}

pub fn process_feed(
    feed: &dmfr::Feed,
    feed_hashmap: &mut HashMap<FeedId, dmfr::Feed>,
    operator_hashmap: &mut HashMap<OperatorId, dmfr::Operator>,
    operator_to_feed_hashmap: &mut HashMap<OperatorId, Vec<FeedPairInfo>>,
    feed_to_operator_pairs_hashmap: &mut HashMap<FeedId, Vec<OperatorPairInfo>>,
) -> () {
    feed_hashmap.entry(feed.id.clone()).or_insert(feed.clone());

    for operator in feed.operators.iter() {
        process_operator(
            &operator,
            feed_hashmap,
            operator_hashmap,
            operator_to_feed_hashmap,
            feed_to_operator_pairs_hashmap,
            Some(&feed.id),
        );

        operator_to_feed_hashmap
            .entry(operator.onestop_id.clone())
            .and_modify(|associated_feeds| {
                let set_of_existing_ids: HashSet<String> = HashSet::from_iter(
                    associated_feeds
                        .iter()
                        .map(|feed_item| feed_item.feed_onestop_id.clone()),
                );

                if !set_of_existing_ids.contains(&feed.id) {
                    associated_feeds.push(FeedPairInfo {
                        feed_onestop_id: feed.id.clone(),
                        gtfs_agency_id: None,
                    });
                }
            })
            .or_insert(vec![FeedPairInfo {
                feed_onestop_id: feed.id.clone(),
                gtfs_agency_id: None,
            }]);

        feed_to_operator_pairs_hashmap
            .entry(feed.id.clone())
            .and_modify(|operator_pairs| {
                let set_of_existing_operator_ids: HashSet<String> = HashSet::from_iter(
                    operator_pairs
                        .iter()
                        .map(|operator_pair| operator_pair.operator_id.clone()),
                );

                if !set_of_existing_operator_ids.contains(&operator.onestop_id.clone()) {
                    operator_pairs.push(OperatorPairInfo {
                        operator_id: operator.onestop_id.clone(),
                        gtfs_agency_id: None,
                    });
                }
            })
            .or_insert(vec![OperatorPairInfo {
                operator_id: operator.onestop_id.clone(),
                gtfs_agency_id: None,
            }]);
    }
}

pub fn process_operator(
    operator: &dmfr::Operator,
    feed_hashmap: &mut HashMap<FeedId, dmfr::Feed>,
    operator_hashmap: &mut HashMap<OperatorId, dmfr::Operator>,
    operator_to_feed_hashmap: &mut HashMap<OperatorId, Vec<FeedPairInfo>>,
    feed_to_operator_pairs_hashmap: &mut HashMap<FeedId, Vec<OperatorPairInfo>>,
    parent_feed_id: Option<&str>,
) -> () {
    operator_hashmap
        .entry(operator.onestop_id.clone())
        .or_insert(operator.clone());

    for associated_feed in operator.associated_feeds.iter() {
        let mut associated_feed_insertion: FeedPairInfo =
            match associated_feed.feed_onestop_id.as_ref() {
                Some(feed_onestop_id) => FeedPairInfo {
                    feed_onestop_id: feed_onestop_id.clone(),
                    gtfs_agency_id: associated_feed.feed_onestop_id.clone(),
                },
                None => FeedPairInfo {
                    feed_onestop_id: String::from(*parent_feed_id.as_ref().unwrap()),
                    gtfs_agency_id: associated_feed.feed_onestop_id.clone(),
                },
            };

        //if associated_feed_insertion.feed_onestop_id == Some(String::from("f-ucla~bruinbus~rt")) {
        //    println!("Bruin realtime feed found! {:?}", associated_feed_insertion);
        //}

        operator_to_feed_hashmap
            .entry(operator.onestop_id.clone())
            .and_modify(|associated_feeds| {
                let set_of_existing_ids: HashSet<String> = HashSet::from_iter(
                    associated_feeds
                        .iter()
                        .map(|feed_item| feed_item.feed_onestop_id.clone()),
                );

                if !set_of_existing_ids.contains(&associated_feed_insertion.feed_onestop_id) {
                    associated_feeds.push(associated_feed_insertion.clone())
                }
            })
            .or_insert(vec![associated_feed_insertion.clone()]);

        feed_to_operator_pairs_hashmap
            .entry(associated_feed_insertion.feed_onestop_id.clone())
            .and_modify(|operator_pairs| {
                let set_of_existing_operator_ids: HashSet<String> = HashSet::from_iter(
                    operator_pairs
                        .iter()
                        .map(|operator_pair| operator_pair.operator_id.clone()),
                );

                if !set_of_existing_operator_ids.contains(&operator.onestop_id.clone()) {
                    operator_pairs.push(OperatorPairInfo {
                        operator_id: operator.onestop_id.clone(),
                        gtfs_agency_id: associated_feed_insertion.gtfs_agency_id.clone(),
                    });
                }
            })
            .or_insert(vec![OperatorPairInfo {
                operator_id: operator.onestop_id.clone(),
                gtfs_agency_id: associated_feed_insertion.gtfs_agency_id.clone(),
            }]);
    }
}

pub fn read_folders(path: &str) -> Result<ReturnDmfrAnalysis, Box<dyn Error>> {
    let feed_entries = fs::read_dir(format!("{}/feeds/", path))?;

    let mut feed_hashmap: HashMap<FeedId, dmfr::Feed> = HashMap::new();
    let mut operator_hashmap: HashMap<OperatorId, dmfr::Operator> = HashMap::new();
    let mut operator_to_feed_hashmap: HashMap<OperatorId, Vec<FeedPairInfo>> = HashMap::new();
    let mut feed_to_operator_pairs_hashmap: HashMap<FeedId, Vec<OperatorPairInfo>> = HashMap::new();

    for entry in feed_entries {
        if let Ok(entry) = entry {
            if let Some(file_name) = entry.file_name().to_str() {
                //println!("{}", file_name);
                let contents = fs::read_to_string(format!("{}/feeds/{}", path, file_name));
                if contents.is_err() {
                    eprintln!(
                        "Error Reading Feed File {}: {}",
                        file_name,
                        contents.unwrap_err()
                    );
                    continue;
                }
                let dmfrinfo: Result<dmfr::DistributedMobilityFeedRegistry, SerdeError> =
                    serde_json::from_str(&contents.unwrap());
                match dmfrinfo {
                    Ok(dmfrinfo) => {
                        for feed in dmfrinfo.feeds.into_iter() {
                            process_feed(
                                &feed,
                                &mut feed_hashmap,
                                &mut operator_hashmap,
                                &mut operator_to_feed_hashmap,
                                &mut feed_to_operator_pairs_hashmap,
                            );
                        }

                        for operator in dmfrinfo.operators.into_iter() {
                            process_operator(
                                &operator,
                                &mut feed_hashmap,
                                &mut operator_hashmap,
                                &mut operator_to_feed_hashmap,
                                &mut feed_to_operator_pairs_hashmap,
                                None,
                            );
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    let operator_entries =
        fs::read_dir(format!("{}/operators/", path)).expect("Transitland atlas missing");

    for operator_file in operator_entries {
        if let Ok(operator_file) = operator_file {
            if let Some(file_name) = operator_file.file_name().to_str() {
                let contents = fs::read_to_string(format!("{}/operators/{}", path, file_name));
                if contents.is_err() {
                    eprintln!(
                        "Error Reading Operator File {}: {}",
                        file_name,
                        contents.unwrap_err()
                    );
                    continue;
                }

                let operator: Result<dmfr::Operator, SerdeError> =
                    serde_json::from_str(&contents.unwrap());

                if let Ok(operator) = operator {
                    process_operator(
                        &operator,
                        &mut feed_hashmap,
                        &mut operator_hashmap,
                        &mut operator_to_feed_hashmap,
                        &mut feed_to_operator_pairs_hashmap,
                        None,
                    );
                }
            }
        }
    }

    let operator_entries = fs::read_dir(format!("{}/operators/switzerland/", path))
        .expect("Transitland atlas missing");

    for operator_file in operator_entries {
        if let Ok(operator_file) = operator_file {
            if let Some(file_name) = operator_file.file_name().to_str() {
                let contents =
                    fs::read_to_string(format!("{}/operators/switzerland/{}", path, file_name));
                if contents.is_err() {
                    eprintln!(
                        "Error Reading Swiss Operator File {}: {}",
                        file_name,
                        contents.unwrap_err()
                    );
                    continue;
                }

                let operator: Result<dmfr::Operator, SerdeError> =
                    serde_json::from_str(&contents.unwrap());

                if let Ok(operator) = operator {
                    process_operator(
                        &operator,
                        &mut feed_hashmap,
                        &mut operator_hashmap,
                        &mut operator_to_feed_hashmap,
                        &mut feed_to_operator_pairs_hashmap,
                        None,
                    );
                }
            }
        }
    }

    //cross check feed_to_operator_hashmap into feed_to_operator_pairs_hashmap

    Ok(ReturnDmfrAnalysis {
        feed_hashmap,
        operator_hashmap,
        operator_to_feed_hashmap,
        feed_to_operator_pairs_hashmap,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let dmfr_result = read_folders("transitland-atlas/");

        assert!(dmfr_result.feed_hashmap.len() > 1000);

        fs::write(
            "operator_to_feed_hashmap.json",
            format!("{:#?}", dmfr_result.operator_to_feed_hashmap),
        )
        .expect("Unable to write file");

        fs::write(
            "feed_to_operator_pairs_hashmap.json",
            format!("{:#?}", dmfr_result.feed_to_operator_pairs_hashmap),
        )
        .expect("Unable to write file");

        println!(
            "{} feeds across {} operators",
            dmfr_result.feed_hashmap.len(),
            dmfr_result.operator_hashmap.len()
        );

        println!(
            "Operator to feed hashmap length {}",
            dmfr_result.operator_to_feed_hashmap.len()
        );
        println!(
            "feed_to_operator_pairs_hashmap length {}",
            dmfr_result.feed_to_operator_pairs_hashmap.len()
        );

        assert!(dmfr_result
            .feed_to_operator_pairs_hashmap
            .get("f-ucla~bruinbus~rt")
            .is_some());
        assert!(dmfr_result
            .feed_to_operator_pairs_hashmap
            .get("f-spokanetransitauthority~rt")
            .is_some());
    }
}
