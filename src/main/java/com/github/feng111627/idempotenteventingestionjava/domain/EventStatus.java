package com.github.feng111627.idempotenteventingestionjava.domain;

/**
 * 事件处理状态。
 * <ul>
 *   <li>{@link #RECEIVED}：已接收，尚未开始下游处理；</li>
 *   <li>{@link #PROCESSING}：正在调用下游（首次或重试）；</li>
 *   <li>{@link #RETRYING}：一次下游调用临时失败，等待退避后重试；</li>
 *   <li>{@link #SUCCESS}：下游已确认落地，终态；</li>
 *   <li>{@link #FAILED}：进入失败区（校验失败/重试耗尽），终态。</li>
 * </ul>
 */
public enum EventStatus {
    RECEIVED,
    PROCESSING,
    RETRYING,
    SUCCESS,
    FAILED
}
