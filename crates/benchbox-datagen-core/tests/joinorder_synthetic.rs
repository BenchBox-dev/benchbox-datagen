use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Int64Array, StringArray};
use benchbox_datagen_core::{
    Benchmark, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn joinorder() -> Benchmark {
    "joinorder_synthetic".parse().unwrap()
}

const TABLES: [&str; 21] = [
    "kind_type",
    "company_type",
    "info_type",
    "role_type",
    "comp_cast_type",
    "link_type",
    "title",
    "name",
    "company_name",
    "keyword",
    "char_name",
    "cast_info",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "person_info",
    "complete_cast",
    "aka_name",
    "aka_title",
];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "kind_type",
        "423b4c5a6c22eb51945fed746610520b07dc6c84219cf2c6b1b8a676e1ce439e",
    ),
    (
        "tbl",
        "0.01",
        "company_type",
        "27f13bddd29d5f8921bbc9710d95c4454cc5e072bc4c1288d8a312a2260e5a26",
    ),
    (
        "tbl",
        "0.01",
        "info_type",
        "4232fb4595f66c947ac3280e3b1be01831196974e9a8384c1f4a5b2b7dd9e766",
    ),
    (
        "tbl",
        "0.01",
        "role_type",
        "2afc2b0dc30042bb5920be6feabd13de79423a144c18efba07c5d9a5ac224100",
    ),
    (
        "tbl",
        "0.01",
        "comp_cast_type",
        "4948ebae6805ccbfab6192f22d51d2a38969d5349ad45bb6927d99fdae869878",
    ),
    (
        "tbl",
        "0.01",
        "link_type",
        "12d359eb45298ac80ab7d914c8b5e23480b7e4f34cf29ec51e072446156da968",
    ),
    (
        "tbl",
        "0.01",
        "title",
        "9875a6147f87992361f64104d190df512281db9df4e42bc3790f63d7485cf301",
    ),
    (
        "tbl",
        "0.01",
        "name",
        "c94fd397d853ae54755bca9c35103a05c85d25fadffcceab5ba569c33ea5305d",
    ),
    (
        "tbl",
        "0.01",
        "company_name",
        "17df4261c28c931bec81ca28e4d0d1e88fd6ee5d4532a6e75414f5ea67015c98",
    ),
    (
        "tbl",
        "0.01",
        "keyword",
        "2e565d00c74443fd7691aee189cd6c724887b9281f2ce247f57df1caaf8bf8f7",
    ),
    (
        "tbl",
        "0.01",
        "char_name",
        "24179a72462266ef2176f1e04e77561bea9d92786e8021e1beff51313ec5bbb3",
    ),
    (
        "tbl",
        "0.01",
        "cast_info",
        "80423f7d5d22cde9bcdef3e6c0e9fa21c2e08e3a2e33e20fdf1983acb02cba9b",
    ),
    (
        "tbl",
        "0.01",
        "movie_companies",
        "47e4e847fce28a23a8cb07f6724982a88b4bfe8983b2fdfcbb73313035d98f8d",
    ),
    (
        "tbl",
        "0.01",
        "movie_info",
        "43b46e5d0e68c8e8c74a286ffacfcbc692e68eca87613993895fcc3a1225f732",
    ),
    (
        "tbl",
        "0.01",
        "movie_info_idx",
        "b519fdeb48075632265e606cecb70f479c74c7694cde818d47a6a1ff9cd5fc3c",
    ),
    (
        "tbl",
        "0.01",
        "movie_keyword",
        "ce76ee2faf2c9bd28369d51b2055ee3a520b8fd0639ce751f7f2af6a3f6cb5fd",
    ),
    (
        "tbl",
        "0.01",
        "movie_link",
        "4da8e7c9fb9d15d197f88a2ff88f8f27e46289df3300a877887a6e51db6a9efb",
    ),
    (
        "tbl",
        "0.01",
        "person_info",
        "2d277bc13ca1a649635ea0b193a55cca5d4291f378aa0a3effac1c8372b70636",
    ),
    (
        "tbl",
        "0.01",
        "complete_cast",
        "8df50d1c80a664a12979490f0cfde9f76579234972a9cc1de87d040f3dd1d40d",
    ),
    (
        "tbl",
        "0.01",
        "aka_name",
        "3aa94d55dd83cc732ab3dc3272c35a044c0a9c51c8a5cd51575200312e7584de",
    ),
    (
        "tbl",
        "0.01",
        "aka_title",
        "27e0fce0709dbf2394a92048148405a37f8a2e1f9f69b31d9ddbb5347b4872be",
    ),
    (
        "tbl",
        "0.1",
        "kind_type",
        "423b4c5a6c22eb51945fed746610520b07dc6c84219cf2c6b1b8a676e1ce439e",
    ),
    (
        "tbl",
        "0.1",
        "company_type",
        "27f13bddd29d5f8921bbc9710d95c4454cc5e072bc4c1288d8a312a2260e5a26",
    ),
    (
        "tbl",
        "0.1",
        "info_type",
        "4232fb4595f66c947ac3280e3b1be01831196974e9a8384c1f4a5b2b7dd9e766",
    ),
    (
        "tbl",
        "0.1",
        "role_type",
        "2afc2b0dc30042bb5920be6feabd13de79423a144c18efba07c5d9a5ac224100",
    ),
    (
        "tbl",
        "0.1",
        "comp_cast_type",
        "4948ebae6805ccbfab6192f22d51d2a38969d5349ad45bb6927d99fdae869878",
    ),
    (
        "tbl",
        "0.1",
        "link_type",
        "12d359eb45298ac80ab7d914c8b5e23480b7e4f34cf29ec51e072446156da968",
    ),
    (
        "tbl",
        "0.1",
        "title",
        "cd6ea6d7352e2ffacb259546998c4f52d72df9ec8769ef66717bb5c93d1d56aa",
    ),
    (
        "tbl",
        "0.1",
        "name",
        "b3c179b28a6977d041e5c31db509108c45fe33089b23eb0a156d44ce68c10cd0",
    ),
    (
        "tbl",
        "0.1",
        "company_name",
        "6bee81dba9a0e0fae31e2e4a45086e3de3aae28d95d6009a2a03a655456c3e76",
    ),
    (
        "tbl",
        "0.1",
        "keyword",
        "525cd1c647b143c20b4f03e8fc7d463e4eaf3ed266e13f3cbcf0671bdbf12515",
    ),
    (
        "tbl",
        "0.1",
        "char_name",
        "f2d5a8dc84c1b99e91fc754456588f2c726cb3600427e29ab34de08855b55883",
    ),
    (
        "tbl",
        "0.1",
        "cast_info",
        "5298a73fb7911cc169137ced2ffe47fca845805a47acdc09c1ad1f1633103715",
    ),
    (
        "tbl",
        "0.1",
        "movie_companies",
        "53d52414880af7b5c4090329d049848f818dd127c823b61a8d9fda6be9a36610",
    ),
    (
        "tbl",
        "0.1",
        "movie_info",
        "8cde9cddf81e05fd6b0a6bb6392771ba5c0f9e88519f4515a32f92d888e8c163",
    ),
    (
        "tbl",
        "0.1",
        "movie_info_idx",
        "702b1d9c76fcc6f2b81fea93cfffc9567dbbe626a8bb73ed72b4c75e89f66ff1",
    ),
    (
        "tbl",
        "0.1",
        "movie_keyword",
        "76ef26deb43b4016d9e91f1f86183abe13838c0440728aa6315c88a690dfb2b5",
    ),
    (
        "tbl",
        "0.1",
        "movie_link",
        "90d31ccc360e4a1622e892852ecfd4c61cbfd4bc72ab1d5b74d4b3ee39576e73",
    ),
    (
        "tbl",
        "0.1",
        "person_info",
        "a4b55231c8e54ee6c421a1ea24f8fa0580c3feff38c736654d30dc430ae95ef7",
    ),
    (
        "tbl",
        "0.1",
        "complete_cast",
        "052aeed2625487fae282091bc93b790b37ff90131026c81dbdd825f4ab50a6b6",
    ),
    (
        "tbl",
        "0.1",
        "aka_name",
        "6a154c96cbe25598f36176505508cfeae827dc1a868fd5a1475e4939977dde61",
    ),
    (
        "tbl",
        "0.1",
        "aka_title",
        "1c23bf9a331dde8c59f4ab4ceef9415e78445e49224767efb220c77c482986b5",
    ),
    (
        "parquet",
        "0.01",
        "kind_type",
        "b6f9687f9a08d9bce4e5120ef2d6612f0ee0f20ab0c79f0c1c5782ea1d0007ca",
    ),
    (
        "parquet",
        "0.01",
        "company_type",
        "cbbd77a2a1e76802504fd5506c69d876f065ffba3ab3c693d71a32007a96dbf3",
    ),
    (
        "parquet",
        "0.01",
        "info_type",
        "f61b0c9122d9514c6b065ee7549cc7bb1b8f77bd8e9ecce7d87bcf25a2d18371",
    ),
    (
        "parquet",
        "0.01",
        "role_type",
        "34fb1a7af572944f59246dbecac092c7a85c44705914e639b085f5092feebb4b",
    ),
    (
        "parquet",
        "0.01",
        "comp_cast_type",
        "27c939269a8f61670fb9c0dff46fc6a07f7558acf0a9fe21fc20bb769dd67659",
    ),
    (
        "parquet",
        "0.01",
        "link_type",
        "c1584947ef2f86e6a7ba2990771ffe919008d7a13794c0085d0b8c7a47c1aaab",
    ),
    (
        "parquet",
        "0.01",
        "title",
        "7b7069b51fbbefd3df3c75d07e821f772d02746b0f044abfb653d817a1e09eb9",
    ),
    (
        "parquet",
        "0.01",
        "name",
        "ba1498c58ea50406f6656829390f360e65d1781a9de7ef0fb6014b4adce35e89",
    ),
    (
        "parquet",
        "0.01",
        "company_name",
        "872765e1335086e0179d293a2ea86c8de14950c99e7a2ac943ac6b5864ef331f",
    ),
    (
        "parquet",
        "0.01",
        "keyword",
        "0bf070813df873b6f9d918e1d6879c5a06711be62fc6a7e166f88f5d32a229d7",
    ),
    (
        "parquet",
        "0.01",
        "char_name",
        "1247bd4d4327a3b3f0028925455380e7d4c49b215230c9e3220b96dfe9fa8516",
    ),
    (
        "parquet",
        "0.01",
        "cast_info",
        "79cdefed71a79e1a29a21c95334f55eb6078ee4428b1d86cfa05434f07f5e2c0",
    ),
    (
        "parquet",
        "0.01",
        "movie_companies",
        "314202a5f1c83e91a5567f8e15e69336efef1c1e78d43ee5f805e39c6c9ca813",
    ),
    (
        "parquet",
        "0.01",
        "movie_info",
        "daca0971063b8bd8641d34a5dcfdca5f575acf6a1d6a36237971b279b875e3ec",
    ),
    (
        "parquet",
        "0.01",
        "movie_info_idx",
        "635180c4d3e5050c6c5d669a2527e32fd182be56dc3e51c873e64a2031070f98",
    ),
    (
        "parquet",
        "0.01",
        "movie_keyword",
        "882cbc0fc01bfd2bd979c79875c40a92d43b542815a35edacefb3229fdf33f7b",
    ),
    (
        "parquet",
        "0.01",
        "movie_link",
        "3b04ade117dbc2417cee1feea6c092da1c788d2248b0fa8eb7a703814619e11f",
    ),
    (
        "parquet",
        "0.01",
        "person_info",
        "97fa4c5ca92242366df3706abd3148bf6f51a2e593fc7532967d0218225865a8",
    ),
    (
        "parquet",
        "0.01",
        "complete_cast",
        "f32d0c250e3b2ddff74b8d75462b8370991c590c414bda7d6d3f381c4330912b",
    ),
    (
        "parquet",
        "0.01",
        "aka_name",
        "8395a9d619e18d9710f0ea531fe0022c18a920280d61d554d4d20b8b279b1e6e",
    ),
    (
        "parquet",
        "0.01",
        "aka_title",
        "ec9b7a2c4459454ce1f60976a6bcae4ae08abbd1f996e1a21e0da11269ab6674",
    ),
    (
        "parquet",
        "0.1",
        "kind_type",
        "b6f9687f9a08d9bce4e5120ef2d6612f0ee0f20ab0c79f0c1c5782ea1d0007ca",
    ),
    (
        "parquet",
        "0.1",
        "company_type",
        "cbbd77a2a1e76802504fd5506c69d876f065ffba3ab3c693d71a32007a96dbf3",
    ),
    (
        "parquet",
        "0.1",
        "info_type",
        "f61b0c9122d9514c6b065ee7549cc7bb1b8f77bd8e9ecce7d87bcf25a2d18371",
    ),
    (
        "parquet",
        "0.1",
        "role_type",
        "34fb1a7af572944f59246dbecac092c7a85c44705914e639b085f5092feebb4b",
    ),
    (
        "parquet",
        "0.1",
        "comp_cast_type",
        "27c939269a8f61670fb9c0dff46fc6a07f7558acf0a9fe21fc20bb769dd67659",
    ),
    (
        "parquet",
        "0.1",
        "link_type",
        "c1584947ef2f86e6a7ba2990771ffe919008d7a13794c0085d0b8c7a47c1aaab",
    ),
    (
        "parquet",
        "0.1",
        "title",
        "d6736e0fdacf3264b5c920178d8ce7b6ca17a4baa05d03c98b47137aa62f6ec1",
    ),
    (
        "parquet",
        "0.1",
        "name",
        "5b804729daf61e5916926f2cf4998e11a873baa2218db96cafb06671f67764d8",
    ),
    (
        "parquet",
        "0.1",
        "company_name",
        "58ed3798573ef70ffcaa2c8113acbb2e40f383176eb050ff5d08a7f0c739d409",
    ),
    (
        "parquet",
        "0.1",
        "keyword",
        "97d48dc5d6118068b34ad0bd94892f7266cbc30380fc29cb4004fbc5bc449b75",
    ),
    (
        "parquet",
        "0.1",
        "char_name",
        "5c54bdc179e45fe536f10b29af7db052d1995854927952b1a4df95bd7bfddb7a",
    ),
    (
        "parquet",
        "0.1",
        "cast_info",
        "c8bab470c2a7fee0dbcd2e2ed58cfbbb614f7d7d5f3279082ec7642d65ac064f",
    ),
    (
        "parquet",
        "0.1",
        "movie_companies",
        "27c512200df2a2debe46893156b75279f3272bff834178b77bcb172982667017",
    ),
    (
        "parquet",
        "0.1",
        "movie_info",
        "263d1fb276f9ec0e6c1a2f6887eb04c03a50529556aa7901b1634bcc4090fb6a",
    ),
    (
        "parquet",
        "0.1",
        "movie_info_idx",
        "1ff2ac8020997d4146ca1ed92c022f781f3534afe7a264511e5d231e22dfc373",
    ),
    (
        "parquet",
        "0.1",
        "movie_keyword",
        "f1171c3ff22cb3a8983c1188b0cb51a7e0fa1ee0ceec19b89d3ac1ad9210a9c2",
    ),
    (
        "parquet",
        "0.1",
        "movie_link",
        "7e8a33c3ce966405f622e97816381a1484de4de6c69fac96f5d50a1b08945bc4",
    ),
    (
        "parquet",
        "0.1",
        "person_info",
        "892a3eefc0485c8dab8f373592286779c9a19f5c5fa9c3a1e8d494868f7535aa",
    ),
    (
        "parquet",
        "0.1",
        "complete_cast",
        "49d1d9746b08b93c13d75bf155bb92a52eb48181e3088e6e1381d140fe72847a",
    ),
    (
        "parquet",
        "0.1",
        "aka_name",
        "d614b394f435846d2acb77e9a9d5ed5070135655c542cf63954f7835916544ec",
    ),
    (
        "parquet",
        "0.1",
        "aka_title",
        "83092e3e870accaf5b6e759db2e4d515979cb56dba6935a60633fb7b608e1c83",
    ),
];

fn expected_rows(scale_factor: f64) -> Vec<u64> {
    let scaled = |base: u64| ((base as f64 * scale_factor) as u64).max(1);
    vec![
        7,
        4,
        24,
        12,
        4,
        18,
        scaled(500_000) + 60,
        scaled(800_000) + 60,
        scaled(60_000) + 60,
        scaled(24_000) + 60,
        scaled(600_000),
        scaled(7_000_000),
        scaled(520_000),
        scaled(3_000_000),
        scaled(280_000),
        scaled(1_000_000),
        scaled(6_000),
        scaled(600_000),
        scaled(30_000),
        scaled(180_000),
        scaled(80_000),
    ]
}

fn request(
    dir: &Path,
    format: Format,
    threads: Option<usize>,
    parts: i32,
    scale_factor: f64,
    seed: Option<u64>,
) -> OutputRequest {
    OutputRequest {
        benchmark: joinorder(),
        scale_factor,
        format,
        compression: None,
        threads: threads.and_then(NonZeroUsize::new),
        parts,
        seed,
        output_dir: dir.to_path_buf(),
    }
}

fn digests(request: &OutputRequest) -> Vec<(String, String)> {
    write_benchmark(request)
        .unwrap()
        .into_iter()
        .map(|stats| {
            let bytes = fs::read(&stats.path).unwrap();
            let hash = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            (
                stats
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                hash,
            )
        })
        .collect()
}

fn table_request(table: &str, scale_factor: f64, seed: Option<u64>) -> TableRequest {
    TableRequest {
        benchmark: joinorder(),
        table: table.to_owned(),
        scale_factor,
        part: 1,
        parts: 1,
        seed,
    }
}

fn whole_table(table: &str, scale_factor: f64) -> arrow::record_batch::RecordBatch {
    let batches: Vec<_> = record_batches(&table_request(table, scale_factor, None))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn int_column(table: &str, scale_factor: f64, column: &str) -> Vec<Option<i64>> {
    let batch = whole_table(table, scale_factor);
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    array.iter().collect()
}

fn text_column(table: &str, scale_factor: f64, column: &str) -> Vec<Option<String>> {
    let batch = whole_table(table, scale_factor);
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    array.iter().map(|value| value.map(str::to_owned)).collect()
}

#[test]
fn row_counts_follow_the_legacy_scale_rules_at_every_supported_scale() {
    for scale_factor in [0.01, 0.1, 0.5, 1.0, 2.0, 10.0] {
        let counts = row_counts(joinorder(), scale_factor).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            TABLES
        );
        assert_eq!(
            counts.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
            expected_rows(scale_factor),
            "sf={scale_factor}"
        );
    }
    let at_one: Vec<u64> = row_counts(joinorder(), 1.0)
        .unwrap()
        .iter()
        .map(|(_, rows)| *rows)
        .collect();
    assert_eq!(at_one[6], 500_060);
    assert_eq!(at_one[11], 7_000_000);
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schemas() {
    for (table, rows) in row_counts(joinorder(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(joinorder(), table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn keys_are_dense_and_foreign_keys_stay_inside_their_ranges() {
    let scale_factor = 0.1;
    for table in TABLES {
        let ids: Vec<i64> = int_column(table, scale_factor, "id")
            .into_iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(ids, (1..=ids.len() as i64).collect::<Vec<_>>(), "{table}");
    }
    let ranges = key_ranges(joinorder(), scale_factor).unwrap();
    let mut checked = 0;
    for range in ranges.iter().filter(|range| range.column != "id") {
        let observed: Vec<i64> = int_column(range.table, scale_factor, range.column)
            .into_iter()
            .flatten()
            .collect();
        assert!(
            observed
                .iter()
                .all(|value| (range.min..=range.max).contains(value)),
            "{}.{}",
            range.table,
            range.column
        );
        checked += 1;
    }
    assert_eq!(checked, 25);
    let titles: HashSet<i64> = int_column("title", scale_factor, "id")
        .into_iter()
        .flatten()
        .collect();
    for (table, column) in [
        ("cast_info", "movie_id"),
        ("movie_companies", "movie_id"),
        ("movie_info", "movie_id"),
        ("movie_keyword", "movie_id"),
        ("movie_link", "linked_movie_id"),
        ("complete_cast", "movie_id"),
    ] {
        assert!(
            int_column(table, scale_factor, column)
                .into_iter()
                .flatten()
                .all(|value| titles.contains(&value)),
            "{table}.{column}"
        );
    }
}

#[test]
fn planted_golden_entities_are_wired_into_the_first_sixty_relationship_rows() {
    let scale_factor = 0.01;
    let titles = int_column("title", scale_factor, "id").len() as i64;
    let movie_ids = int_column("cast_info", scale_factor, "movie_id");
    for (index, movie) in movie_ids.iter().take(60).enumerate() {
        assert_eq!(*movie, Some(titles - 59 + index as i64));
    }
    let names = text_column("title", scale_factor, "title");
    assert_eq!(
        names[titles as usize - 60].as_deref(),
        Some("Golden Adventure 0")
    );
    let notes = text_column("cast_info", scale_factor, "note");
    assert_eq!(notes[0].as_deref(), Some("(voice) (uncredited)"));
    assert_eq!(notes[2].as_deref(), Some("(producer)"));
    let infos = text_column("movie_info", scale_factor, "info");
    assert_eq!(infos[0].as_deref(), Some("Sweden"));
    assert_eq!(infos[1].as_deref(), Some("Germany"));
    let episodes = int_column("title", scale_factor, "episode_nr");
    assert_eq!(episodes[titles as usize - 60], Some(50));
    assert_eq!(episodes[titles as usize - 59], None);
}

#[test]
fn csv_output_has_no_header_and_writes_nulls_as_empty_fields() {
    let dir = tempfile::tempdir().unwrap();
    write_benchmark(&request(dir.path(), Format::Tbl, Some(1), 1, 0.01, None)).unwrap();
    let kinds = fs::read_to_string(dir.path().join("kind_type.csv")).unwrap();
    assert!(kinds.starts_with("1,movie\n2,tv series\n"));
    let titles = fs::read_to_string(dir.path().join("title.csv")).unwrap();
    let first = titles.lines().next().unwrap();
    assert_eq!(first.split(',').count(), 12);
    assert!(first.starts_with("1,"));
    let aka = fs::read_to_string(dir.path().join("aka_title.csv")).unwrap();
    assert!(aka.lines().next().unwrap().contains(",,,"));
    assert!(!dir.path().join("title.tbl").exists());
}

#[test]
fn files_are_identical_for_one_four_and_default_threads() {
    for format in [Format::Tbl, Format::Parquet] {
        let runs: Vec<_> = [Some(1), Some(4), None]
            .into_iter()
            .map(|threads| {
                let dir = tempfile::tempdir().unwrap();
                digests(&request(dir.path(), format, threads, 1, 0.1, None))
            })
            .collect();
        assert_eq!(runs[0], runs[1], "{format:?}");
        assert_eq!(runs[0], runs[2], "{format:?}");
    }
}

#[test]
fn delimited_parts_concatenate_to_the_single_file() {
    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(whole.path(), Format::Tbl, Some(2), 1, 0.1, None)).unwrap();
    let stats =
        write_benchmark(&request(split.path(), Format::Tbl, Some(3), 3, 0.1, None)).unwrap();
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(files.len(), 3, "{table}");
        let joined: Vec<u8> = files
            .iter()
            .flat_map(|file| fs::read(&file.path).unwrap())
            .collect();
        assert_eq!(
            joined,
            fs::read(whole.path().join(format!("{table}.csv"))).unwrap(),
            "{table}"
        );
    }
}

#[test]
fn seeds_select_the_stream_and_default_to_42() {
    let hash = |seed| {
        let dir = tempfile::tempdir().unwrap();
        digests(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, seed))
    };
    assert_eq!(hash(None), hash(Some(42)));
    let other = hash(Some(43));
    let default = hash(None);
    let constant = [
        "kind_type",
        "company_type",
        "info_type",
        "role_type",
        "comp_cast_type",
        "link_type",
        "char_name",
        "movie_link",
    ];
    for (index, table) in TABLES.iter().enumerate() {
        if constant.contains(table) {
            assert_eq!(other[index], default[index], "{table}");
        } else {
            assert_ne!(other[index], default[index], "{table}");
        }
    }
}

#[test]
fn golden_hashes_are_frozen() {
    for (format, scale, name) in [
        (Format::Tbl, 0.01, "tbl"),
        (Format::Tbl, 0.1, "tbl"),
        (Format::Parquet, 0.01, "parquet"),
        (Format::Parquet, 0.1, "parquet"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let actual = digests(&request(dir.path(), format, Some(3), 1, scale, None));
        for (file, hash) in actual {
            let table = file.split('.').next().unwrap();
            let expected = GOLDENS
                .iter()
                .find(|(f, s, t, _)| *f == name && *s == scale.to_string() && *t == table)
                .unwrap_or_else(|| panic!("missing golden {name} {scale} {table}: {hash}"));
            assert_eq!(expected.3, hash, "{name} sf={scale} {table}");
        }
    }
}

fn part_table(
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> arrow::record_batch::RecordBatch {
    let request = TableRequest {
        part,
        parts,
        ..table_request(table, scale_factor, None)
    };
    let reader = record_batches(&request).unwrap();
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

#[test]
fn generate_table_parts_equal_the_matching_slice_of_the_whole_table() {
    let parts = 3;
    for table in ["title", "cast_info", "movie_info", "aka_name"] {
        let whole = whole_table(table, 0.1);
        let rows = whole.num_rows() as u128;
        for part in 1..=parts {
            let start = (rows * (part as u128 - 1) / parts as u128) as usize;
            let end = (rows * part as u128 / parts as u128) as usize;
            assert_eq!(
                part_table(table, 0.1, part, parts),
                whole.slice(start, end - start),
                "{table} part {part}"
            );
        }
    }
}

#[test]
fn parquet_parts_hold_the_rows_of_the_single_file() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(
        whole.path(),
        Format::Parquet,
        Some(2),
        1,
        0.1,
        None,
    ))
    .unwrap();
    let stats = write_benchmark(&request(
        split.path(),
        Format::Parquet,
        Some(3),
        3,
        0.1,
        None,
    ))
    .unwrap();
    let read = |path: &Path| {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        reader.map(Result::unwrap).collect::<Vec<_>>()
    };
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(files.len(), 3, "{table}");
        let expected = read(&whole.path().join(format!("{table}.parquet")));
        let schema = expected[0].schema();
        let expected = arrow::compute::concat_batches(&schema, &expected).unwrap();
        let joined: Vec<_> = files.iter().flat_map(|file| read(&file.path)).collect();
        assert_eq!(
            arrow::compute::concat_batches(&schema, &joined).unwrap(),
            expected,
            "{table}"
        );
        assert_eq!(
            files.iter().map(|file| file.rows).sum::<u64>(),
            expected.num_rows() as u64
        );
    }
}
