package org.nupsea.flink.batch.student

case class StudentScore(
                       student: String,
                       subject: String,
                       classScore: Float,
                       testScore: Float
                       )
