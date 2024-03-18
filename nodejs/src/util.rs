use lancedb::DistanceType;

pub fn parse_distance_type(distance_type: impl AsRef<str>) -> napi::Result<DistanceType> {
    match distance_type.as_ref().to_lowercase().as_str() {
        "l2" => Ok(DistanceType::L2),
        "cosine" => Ok(DistanceType::Cosine),
        "dot" => Ok(DistanceType::Dot),
        _ => Err(napi::Error::from_reason(format!(
            "Invalid distance type '{}'.  Must be one of l2, cosine, or dot",
            distance_type.as_ref()
        ))),
    }
}
