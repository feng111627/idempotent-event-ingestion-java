package com.github.feng111627.idempotenteventingestionjava.downstream;

/**
 * 下游落地结果。
 * <ul>
 *   <li>{@link #LANDED}：本次首次落地成功；</li>
 *   <li>{@link #ALREADY_LANDED}：下游去重命中（如重启后补偿），未重复写入。</li>
 * </ul>
 */
public enum DeliveryResult {
    LANDED,
    ALREADY_LANDED
}
