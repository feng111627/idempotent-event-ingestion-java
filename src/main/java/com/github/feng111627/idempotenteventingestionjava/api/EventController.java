package com.github.feng111627.idempotenteventingestionjava.api;

import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.IngestResult;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventIngestionService;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;

/** 事件接入与状态查询接口。 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventIngestionService ingestionService;
    private final EventStore store;

    public EventController(EventIngestionService ingestionService, EventStore store) {
        this.ingestionService = ingestionService;
        this.store = store;
    }

    /** 接入事件：200=新事件已处理，200(DUPLICATE)=重复投递，400=校验失败。 */
    @PostMapping
    public ResponseEntity<IngestResponse> ingest(@RequestBody IngestRequest request) {
        IngestResponse response = ingestionService.ingest(request);
        HttpStatus status = response.result() == IngestResult.REJECTED ? HttpStatus.BAD_REQUEST : HttpStatus.OK;
        return ResponseEntity.status(status).body(response);
    }

    /** 单事件状态查询。 */
    @GetMapping("/{eventId}")
    public ResponseEntity<EventStatusView> get(@PathVariable String eventId) {
        return store.find(eventId)
                .map(record -> ResponseEntity.ok(toView(record)))
                .orElse(ResponseEntity.notFound().build());
    }

    /** 全部事件的一致状态视图。 */
    @GetMapping
    public List<EventStatusView> list() {
        return store.findAll().stream()
                .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                .map(this::toView)
                .toList();
    }

    /** 失败区（死信）：持续失败或校验失败的事件。 */
    @GetMapping("/failures")
    public List<EventStatusView> failures() {
        return store.findAll().stream()
                .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER)
                .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                .map(this::toView)
                .toList();
    }

    private EventStatusView toView(EventRecord r) {
        return new EventStatusView(r.getEventId(), r.getStatus(), r.getAttempts(),
                r.getDuplicateAttempts(), r.getFailureReason(), r.getFailureDetail(),
                r.getCreatedAt(), r.getUpdatedAt());
    }
}
