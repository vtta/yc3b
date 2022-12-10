use std::{fs, path::Path, time::Duration};

use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Distribution {
    Constant,
    Uniform,
    Zipfian,
    Latest,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum InsertOrder {
    Hashed,
    Ordered,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MeasurementType {
    Histogram,
    Timeseries,
    Raw,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct HistogramConfig {
    /// The range of latencies to track in the histogram (milliseconds)
    #[serde_as(as = "DurationMilliSeconds")]
    buckets: Duration,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct TimeseriesConfig {
    /// Granularity for time series (in milliseconds)
    #[serde_as(as = "DurationMilliSeconds")]
    granularity: Duration,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Clone, Builder)]
#[builder(pattern = "owned", default)]
#[serde(default)]
pub struct Workload {
    /// The name of the workload class to use
    workload: String,
    /// The number of records in the table to be inserted in
    /// the load phase or the number of records already in the
    /// table before the run phase
    /// (required to be set)
    #[serde(rename = "recordcount")]
    record_count: u64,
    /// The number of operations to use during the run phase.
    /// (required to be set)
    #[serde(rename = "operationcount")]
    operation_count: u64,
    /// The number of thread
    #[serde(rename = "threadcount")]
    thread_count: u64,
    /// The number of insertions to do, if different from recordcount.
    /// Used with insertstart to grow an existing table
    #[serde(rename = "insertcount")]
    insert_count: u64,
    /// The offset of the first insertion
    #[serde(rename = "insertstart")]
    insert_start: u64,
    /// The number of fields in a record
    #[serde(rename = "fieldcount")]
    field_count: u64,
    /// The size of each field (in bytes)
    #[serde(rename = "fieldlength")]
    field_length: u64,
    /// Should read all field
    #[serde(rename = "readallfields")]
    read_all_fields: bool,
    /// Should write all fields on update
    #[serde(rename = "writeallfields")]
    write_all_fields: bool,
    /// The distribution usd to choose the lenght of a field
    /// (could be: constant, uniform, zipfian, ~~latest~~)
    #[serde(rename = "fieldlengthdistribution")]
    field_length_distribution: Distribution,
    /// What proportion of operations are reads
    #[serde(rename = "readproportion")]
    read_proportion: f64,
    /// What proportion of operations are updates
    #[serde(rename = "updateproportion")]
    update_proportion: f64,
    /// What proportion of operations are inserts
    #[serde(rename = "insertproportion")]
    insert_proportion: f64,
    /// What proportion of operations read then modify a record
    #[serde(rename = "readmodifywriteproportion")]
    read_modify_write_proportion: f64,
    /// What proportion of operations are scans
    #[serde(rename = "scanproportion")]
    scan_proportion: f64,
    /// On a single scan, the maximum number of records to access
    #[serde(rename = "maxscanlength")]
    max_scan_length: u64,
    /// The distribution used to choose the number of records to access on a scan
    /// (could be: ~~constant~~, uniform, zipfian, ~~latest~~)
    #[serde(rename = "scanlengthdistribution")]
    scan_length_distribution: Distribution,
    /// Should records be inserted in order or pseudo-randomly
    #[serde(rename = "insertorder")]
    insert_order: InsertOrder,
    /// The distribution of requests across the keyspace
    /// (could be: ~~constant~~, uniform, zipfian, latest)
    #[serde(rename = "requestdistribution")]
    request_distribution: Distribution,
    /// Percentage of data items that constitute the hot set
    #[serde(rename = "readcount")]
    hotspot_data_fraction: f64,
    /// Percentage of operations that access the hot set
    #[serde(rename = "hotspotopnfraction")]
    hotspot_operation_fraction: f64,
    /// Maximum execution time in seconds
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "maxexecutiontime")]
    max_execution_time: Duration,
    /// The name of the database table to run queries against
    table: String,
    /// The column family of fields (required by some databases)
    #[serde(rename = "columnfamily")]
    column_family: String,
    /// How the latency measurements are presented
    ///(could be: histogram, timeseries, raw)
    #[serde(rename = "measurementtype")]
    measurement_type: MeasurementType,
    histogram: HistogramConfig,
    timeseries: TimeseriesConfig,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            workload: "core".to_owned(),
            record_count: 1000000,
            operation_count: 3000000,
            thread_count: 500,
            insert_count: 0,
            insert_start: 0,
            field_count: 10,
            field_length: 100,
            read_all_fields: true,
            write_all_fields: false,
            field_length_distribution: Distribution::Constant,
            read_proportion: 0.95,
            update_proportion: 0.05,
            insert_proportion: 0.,
            read_modify_write_proportion: 0.,
            scan_proportion: 0.,
            max_scan_length: 1000,
            scan_length_distribution: Distribution::Uniform,
            insert_order: InsertOrder::Hashed,
            request_distribution: Distribution::Zipfian,
            hotspot_data_fraction: 0.2,
            hotspot_operation_fraction: 0.8,
            max_execution_time: Duration::from_secs(0),
            table: "usertable".to_owned(),
            column_family: "".to_owned(),
            measurement_type: MeasurementType::Histogram,
            histogram: HistogramConfig {
                buckets: Duration::from_millis(1000),
            },
            timeseries: TimeseriesConfig {
                granularity: Duration::from_millis(1000),
            },
        }
    }
}

impl Workload {
    pub fn from_toml_str(toml: &str) -> Self {
        toml::from_str(toml).unwrap()
    }
    pub fn from_toml_file<P: AsRef<Path>>(path: P) -> Self {
        Workload::from_toml_str(&fs::read_to_string(path).unwrap())
    }
    pub fn a(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.)
            .read_proportion(0.5)
            .scan_proportion(0.)
            .update_proportion(0.5)
            .request_distribution(Distribution::Uniform)
            .build()
            .unwrap()
    }
    pub fn b(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.)
            .read_proportion(0.95)
            .scan_proportion(0.)
            .update_proportion(0.05)
            .request_distribution(Distribution::Uniform)
            .build()
            .unwrap()
    }
    pub fn c(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.)
            .read_proportion(1.)
            .scan_proportion(0.)
            .update_proportion(0.)
            .request_distribution(Distribution::Uniform)
            .build()
            .unwrap()
    }
    pub fn d(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.05)
            .read_proportion(0.95)
            .scan_proportion(0.)
            .update_proportion(0.)
            .request_distribution(Distribution::Latest)
            .build()
            .unwrap()
    }
    pub fn e(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.05)
            .read_proportion(0.)
            .scan_proportion(0.95)
            .update_proportion(0.)
            .request_distribution(Distribution::Uniform)
            .max_scan_length(1)
            .scan_length_distribution(Distribution::Uniform)
            .build()
            .unwrap()
    }
    pub fn f(record_count: u64, operation_count: u64) -> Self {
        WorkloadBuilder::default()
            .record_count(record_count)
            .operation_count(operation_count)
            .read_all_fields(true)
            .insert_proportion(0.)
            .read_modify_write_proportion(0.5)
            .read_proportion(0.5)
            .scan_proportion(0.)
            .update_proportion(0.)
            .request_distribution(Distribution::Uniform)
            .build()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    const DEFAULT_CONFIG_STRING: &str = r#"
workload = "core"
recordcount = 1000000
operationcount = 3000000
threadcount = 500
insertcount = 0
insertstart = 0
fieldcount = 10
fieldlength = 100
readallfields = true
writeallfields = false
fieldlengthdistribution = "constant"
readproportion = 0.95
updateproportion = 0.05
insertproportion = 0.0
readmodifywriteproportion = 0.0
scanproportion = 0.0
maxscanlength = 1000
scanlengthdistribution = "uniform"
insertorder = "hashed"
requestdistribution = "zipfian"
readcount = 0.2
hotspotopnfraction = 0.8
maxexecutiontime = 0
table = "usertable"
columnfamily = ""
measurementtype = "histogram"

[histogram]
buckets = 1000

[timeseries]
granularity = 1000"#;

    use super::*;
    #[test]
    fn workload_serialization() {
        assert_eq!(
            DEFAULT_CONFIG_STRING.trim(),
            toml::to_string(&Workload::default()).unwrap().trim()
        )
    }

    #[test]
    fn workload_deserialization() {
        let config_str = fs::read_to_string("workloads/workload_template.toml").unwrap();
        let config: Workload = toml::from_str(&config_str).unwrap();
        assert_eq!(
            DEFAULT_CONFIG_STRING.trim(),
            toml::to_string(&config).unwrap().trim()
        );
    }

    #[test]
    fn workloada() {
        assert_eq!(
            toml::to_string(&Workload::a(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloada.toml"))
        )
    }

    #[test]
    fn workloadb() {
        assert_eq!(
            toml::to_string(&Workload::b(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloadb.toml"))
        )
    }

    #[test]
    fn workloadc() {
        assert_eq!(
            toml::to_string(&Workload::c(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloadc.toml"))
        )
    }

    #[test]
    fn workloadd() {
        assert_eq!(
            toml::to_string(&Workload::d(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloadd.toml"))
        )
    }

    #[test]
    fn workloade() {
        assert_eq!(
            toml::to_string(&Workload::e(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloade.toml"))
        )
    }

    #[test]
    fn workloadf() {
        assert_eq!(
            toml::to_string(&Workload::f(1000, 1000)),
            toml::to_string(&Workload::from_toml_file("workloads/workloadf.toml"))
        )
    }
}

// pub(crate) fn project_root() -> PathBuf {
//     let dir = env!("CARGO_MANIFEST_DIR");
//     PathBuf::from(dir)
//         .parent()
//         .unwrap()
//         .parent()
//         .unwrap()
//         .to_owned()
// }
