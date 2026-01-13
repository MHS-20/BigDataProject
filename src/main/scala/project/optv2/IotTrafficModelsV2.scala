package project.optv2

case class IPProfileEnriched(
                              id_orig_h: String,
                              avg_bytes_sent: Double,
                              total_bytes_sent: Long,
                              connection_count: Long,
                              avg_duration: Double,
                              traffic_class: String,
                              benign_count: Long,
                              malicious_count: Long,
                              benign_percent: Double,
                              malicious_percent: Double
                            )