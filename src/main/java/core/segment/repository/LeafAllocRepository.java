package core.segment.repository;

import core.segment.entity.LeafAlloc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface LeafAllocRepository extends JpaRepository<LeafAlloc, String> {
    @Modifying
    @Transactional
    @Query("UPDATE LeafAlloc la SET la.maxId = la.maxId + la.step WHERE la.bizTag = :bizTag")
    int updateMaxId(@Param("bizTag") String bizTag);

    Optional<LeafAlloc> findByBizTag(@Param("bizTag") String bizTag);
}