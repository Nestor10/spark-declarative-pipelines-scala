package dev.sdp.connect

import zio.test.*

/** Transport configuration: parsing and the channel-builder DECISION
  * (review item 5).
  *
  * There is no live TLS endpoint to test against, so the unit under test is the
  * pure decision — `TransportConfig.parse` (raw env/setting strings → config)
  * and `TransportConfig.plan` (config → what the channel builder must do). The
  * socket itself is exercised by the container-gated integration suites in
  * plaintext mode, which these defaults must leave untouched.
  */
object ConnectTransportSpec extends ZIOSpecDefault:

  private val Secret = "super-secret-pat-value"

  def spec = suite("Spark Connect transport configuration")(
    test("plaintext + anonymous is the default (the localhost dev container)") {
      val config = TransportConfig.parse(useTls = None, token = None)
      assertTrue(
        config == Right(TransportConfig.plaintext),
        config.map(_.useTls) == Right(false),
        config.map(_.token) == Right(None),
        config.map(_.deadlineSeconds) == Right(TransportConfig.DefaultDeadlineSeconds),
      )
    },
    test("an empty or blank value is treated as unset") {
      assertTrue(
        TransportConfig.parse(Some(""), Some("")) == Right(TransportConfig.plaintext),
        TransportConfig.parse(Some("  "), Some("   ")) == Right(TransportConfig.plaintext),
      )
    },
    test("the TLS flag accepts the usual spellings, in any case") {
      val on  = List("true", "TRUE", "1", "yes", "on", " True ")
      val off = List("false", "FALSE", "0", "no", "off")
      assertTrue(
        on.forall(v => TransportConfig.parse(Some(v), None).map(_.useTls) == Right(true)),
        off.forall(v => TransportConfig.parse(Some(v), None).map(_.useTls) == Right(false)),
      )
    },
    test("a malformed TLS flag is a Left naming the variable and the value") {
      val result = TransportConfig.parse(Some("maybe"), None)
      assertTrue(
        result.isLeft,
        result.swap.exists(_.contains("SDP_CONNECT_USE_TLS")),
        result.swap.exists(_.contains("maybe")),
        // the setting name is caller-supplied, so the plugin can name ITS setting
        TransportConfig
          .parse(Some("maybe"), None, tlsVarName = "sdpConnectUseTls")
          .swap
          .exists(_.contains("sdpConnectUseTls")),
      )
    },
    test("a token is trimmed and kept; deadlines must be positive") {
      assertTrue(
        TransportConfig.parse(None, Some(s" $Secret ")).map(_.token) == Right(Some(Secret)),
        TransportConfig.parse(None, None, Some("30")).map(_.deadlineSeconds) == Right(30L),
        TransportConfig.parse(None, None, Some("0")).isLeft,
        TransportConfig.parse(None, None, Some("-5")).isLeft,
        TransportConfig.parse(None, None, Some("soon")).swap.exists(_.contains("soon")),
      )
    },
    test("the channel plan: plaintext, no bearer, default deadline") {
      val plan = TransportConfig.plan("localhost", 15002, TransportConfig.plaintext)
      assertTrue(
        plan == TransportConfig.ChannelPlan(
          "localhost",
          15002,
          TransportConfig.Security.Plaintext,
          bearer = false,
          deadlineSeconds = TransportConfig.DefaultDeadlineSeconds,
        )
      )
    },
    test("the channel plan: TLS and a bearer credential when configured") {
      val config = TransportConfig(useTls = true, token = Some(Secret), deadlineSeconds = 15L)
      val plan   = TransportConfig.plan("dbc.example.com", 443, config)
      assertTrue(
        plan.security == TransportConfig.Security.Tls,
        plan.bearer,
        plan.deadlineSeconds == 15L,
        plan.host == "dbc.example.com",
        plan.port == 443,
      )
    },
    test("TLS without a token, and a token without TLS, are both expressible") {
      assertTrue(
        TransportConfig.plan("h", 443, TransportConfig(useTls = true)).bearer == false,
        TransportConfig
          .plan("h", 15002, TransportConfig(token = Some(Secret)))
          .security == TransportConfig.Security.Plaintext,
      )
    },
    test("the token is NEVER rendered — not in toString, not in the plan") {
      val config = TransportConfig(useTls = true, token = Some(Secret))
      val plan   = TransportConfig.plan("h", 443, config)
      assertTrue(
        !config.toString.contains(Secret),
        config.toString.contains("<redacted>"),
        !plan.toString.contains(Secret),
        // and a config embedded in a larger structure stays redacted
        !s"config=$config".contains(Secret),
      )
    },
  )
