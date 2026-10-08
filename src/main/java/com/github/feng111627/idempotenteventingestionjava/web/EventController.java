package com.github.feng111627.idempotenteventingestionjava.web;

import com.github.feng111627.idempotenteventingestionjava.service.EventProcessingService;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventRequest;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventResponse;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventStatusView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 事件接入 REST 接口。
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final EventProcessingService service;

    public EventController(EventProcessingService service) {
        this.service = service;
    }

    /**
     * 投递事件。
     * 首次投递：202 Accepted，响应 duplicate=false；
     * 重复投递：200 OK，响应 duplicate=true 并带当前状态，绝不静默丢弃、绝不重复落地。
     */
    @PostMapping
    public ResponseEntity<EventResponse> receive(@RequestBody EventRequest request) {
        EventResponse response = service.receive(request);
        log.info("事件投递受理完成 eventId={}, duplicate={}, status={}, httpStatus={}",
                response.eventId(), response.duplicate(), response.status(), response.httpStatusHint());
        return ResponseEntity.status(response.httpStatusHint()).body(response);
    }

    /** 单事件一致状态查询；不存在返回 404（由全局异常处理统一出参）。 */
    @GetMapping("/{eventId}")
    public EventStatusView query(@PathVariable String eventId) {
        return service.query(eventId);
    }

    /** 全部事件状态视图（成功/重试中/失败统一视图）。 */
    @GetMapping
    public List<EventStatusView> all() {
        return service.queryAll();
    }

    /** 失败区查询：独立路径，便于运营排查失败原因分类。 */
    @GetMapping("/failures")
    public List<EventStatusView> failures() {
        return service.queryFailed();
    }
}
