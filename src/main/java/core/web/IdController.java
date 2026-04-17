package core.web;

import core.IdGenerate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/id")
public class IdController {

    @Autowired
    @Qualifier("snowflakeIdGenerate")
    private IdGenerate snowflakeIdGenerate;

    @Autowired
    @Qualifier("segmentIdGenerate")
    private IdGenerate segmentIdGenerate;

    /**
     * 生成雪花 ID（不依赖业务 key）
     * GET /api/id/snowflake
     */
    @GetMapping("/snowflake")
    public ResponseEntity<Long> snowflake() {
        return ResponseEntity.ok(snowflakeIdGenerate.getId(null));
    }

    /**
     * 生成顺序 ID（依赖业务 key）
     * GET /api/id/segment/{key}
     */
    @GetMapping("/segment/{key}")
    public ResponseEntity<Long> segment(@PathVariable String key) {
        return ResponseEntity.ok(segmentIdGenerate.getId(key));
    }
}
