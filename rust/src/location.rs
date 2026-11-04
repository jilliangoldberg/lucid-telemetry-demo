//! The approved location pattern (privacy/POLICY.md): VIN-linked location may leave a job
//! only as a ~1 km cell, lat/lon to 2 decimals. Same output as legacy/Privacy.scala.

pub fn cell(lat: f64, lon: f64) -> String {
    format!("{lat:.2},{lon:.2}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn cell_has_two_decimals() {
        assert_eq!(super::cell(37.774_93, -122.419_42), "37.77,-122.42");
    }
}
