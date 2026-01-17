package project

case class TrafficRecord(
                          ts: Double,
                          uid: String,
                          id_orig_h: String,
                          id_orig_p: Int,
                          id_resp_h: String,
                          id_resp_p: Int,
                          proto: String,
                          service: String,
                          duration: Double,
                          orig_bytes: Long,
                          resp_bytes: Long,
                          conn_state: String,
                          local_orig: String,
                          local_resp: String,
                          missed_bytes: Long,
                          history: String,
                          orig_pkts: Int,
                          orig_ip_bytes: Long,
                          resp_pkts: Int,
                          resp_ip_bytes: Long,
                          tunnel_parents: String,
                          label: String,
                          timestamp: String,
                          date: String,
                          hour: Int
                        )

case class IPProfile(
                      id_orig_h: String,
                      avg_bytes_sent: Double,
                      total_bytes_sent: Long,
                      connection_count: Long,
                      avg_duration: Double,
                      traffic_class: String
                    )

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

case class EnrichedRecord(
                           record: TrafficRecord,
                           profile: IPProfile
                         )

case class TrafficClassStats(
                              traffic_class: String,
                              benign: Long,
                              malicious: Long,
                              total: Long,
                              benign_percent: Double,
                              malicious_percent: Double
                            )

case class CategoryStats(
                          traffic_class: String,
                          label: String,
                          avg_bytes: Double,
                          avg_duration: Double,
                          avg_packets: Double,
                          count: Long
                        )