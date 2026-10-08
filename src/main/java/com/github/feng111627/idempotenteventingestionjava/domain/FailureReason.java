package com.github.feng111627.idempotenteventingestionjava.domain;

/**
 * 失败原因分类。对调用方只暴露分类码与可读信息，不透出底层异常。
 * <ul>
 *   <li>{@link #VALIDATION_FAILED}：事件内容校验未通过（不重试，直接进失败区）；</li>
 *   <li>{@link #DOWNSTREAM_UNAVAILABLE}：下游持续不可用，重试耗尽后进入失败区；</li>
 *   <li>{@link #DUPLICATE}：重复投递（仅用于失败区查询时区分，重复请求本身不写入失败区）。</li>
 * </ul>
 */
public enum FailureReason {
    VALIDATION_FAILED,
    DOWNSTREAM_UNAVAILABLE,
    DUPLICATE
}
