import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

def create_kafka_presentation():
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    # Clean Modern Dark Slate Palette
    COLOR_BG = RGBColor(11, 19, 43)             # Dark Slate Navy #0B132B
    COLOR_CARD_BG = RGBColor(28, 37, 65)        # Deep Blue Charcoal #1C2541
    COLOR_CARD_BORDER = RGBColor(58, 80, 107)   # Slate Border #3A506B
    COLOR_CARD_HEADER = RGBColor(38, 51, 85)   # Header Container #263355
    
    COLOR_ORANGE = RGBColor(255, 107, 0)        # Kafka Primary Orange #FF6B00
    COLOR_AMBER = RGBColor(245, 158, 11)        # Accent Amber #F59E0B
    COLOR_CYAN = RGBColor(0, 180, 216)          # Electric Cyan #00B4D8
    COLOR_GREEN = RGBColor(16, 185, 129)        # Emerald Green #10B981
    COLOR_PURPLE = RGBColor(168, 85, 247)       # Accent Purple #A855F7
    
    COLOR_TEXT_WHITE = RGBColor(248, 250, 252)  # Crisp White #F8FAFC
    COLOR_TEXT_MUTED = RGBColor(148, 163, 184)  # Slate Gray #94A3B8
    COLOR_TEXT_DIM = RGBColor(203, 213, 225)    # Light Gray #CBD5E1

    def set_slide_background(slide):
        bg = slide.background
        fill = bg.fill
        fill.solid()
        fill.fore_color.rgb = COLOR_BG

    def add_card(slide, left, top, width, height, bg_color=COLOR_CARD_BG, border_color=COLOR_CARD_BORDER):
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        shape.fill.solid()
        shape.fill.fore_color.rgb = bg_color
        if border_color:
            shape.line.color.rgb = border_color
            shape.line.width = Pt(1.5)
        else:
            shape.line.fill.background()
        return shape

    def add_header(slide, title_text, badge_text="SYSTEM ARCHITECTURE"):
        # Category Badge
        badge_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(10), Inches(0.35))
        tf_b = badge_box.text_frame
        tf_b.word_wrap = True
        tf_b.margin_left = tf_b.margin_top = tf_b.margin_right = tf_b.margin_bottom = 0
        p_b = tf_b.paragraphs[0]
        p_b.text = badge_text.upper()
        p_b.font.size = Pt(11)
        p_b.font.bold = True
        p_b.font.color.rgb = COLOR_ORANGE
        p_b.font.name = "Arial"

        # Slide Title
        title_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.72), Inches(11.5), Inches(0.7))
        tf_t = title_box.text_frame
        tf_t.word_wrap = True
        tf_t.margin_left = tf_t.margin_top = tf_t.margin_right = tf_t.margin_bottom = 0
        p_t = tf_t.paragraphs[0]
        p_t.text = title_text
        p_t.font.size = Pt(25)
        p_t.font.bold = True
        p_t.font.color.rgb = COLOR_TEXT_WHITE
        p_t.font.name = "Arial"

    def add_bullet_point(tf, title, body, accent_color=COLOR_CYAN, space_after=12):
        p = tf.add_paragraph() if tf.paragraphs[0].text else tf.paragraphs[0]
        p.space_after = Pt(space_after)
        
        run_title = p.add_run()
        run_title.text = title + ": "
        run_title.font.bold = True
        run_title.font.size = Pt(13)
        run_title.font.color.rgb = accent_color
        run_title.font.name = "Arial"

        run_body = p.add_run()
        run_body.text = body
        run_body.font.size = Pt(13)
        run_body.font.color.rgb = COLOR_TEXT_DIM
        run_body.font.name = "Arial"

    # =========================================================================
    # SLIDE 1: Title Slide (Cover)
    # =========================================================================
    slide1 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide1)

    # Main Hero Container Card
    add_card(slide1, Inches(0.8), Inches(0.8), Inches(11.733), Inches(3.2), bg_color=COLOR_CARD_BG, border_color=COLOR_ORANGE)
    
    tbox1 = slide1.shapes.add_textbox(Inches(1.2), Inches(1.1), Inches(11), Inches(2.6))
    tf1 = tbox1.text_frame
    tf1.word_wrap = True
    
    p0 = tf1.paragraphs[0]
    p0.text = "DATA ENGINEERING & DISTRIBUTED SYSTEMS"
    p0.font.size = Pt(13)
    p0.font.bold = True
    p0.font.color.rgb = COLOR_ORANGE
    p0.space_after = Pt(8)
    
    p1 = tf1.add_paragraph()
    p1.text = "Architecture of Apache Kafka"
    p1.font.size = Pt(38)
    p1.font.bold = True
    p1.font.color.rgb = COLOR_TEXT_WHITE
    p1.space_after = Pt(10)

    p2 = tf1.add_paragraph()
    p2.text = "High-Throughput Distributed Event Streaming, Storage Internals & Fault-Tolerant Design"
    p2.font.size = Pt(16)
    p2.font.color.rgb = COLOR_TEXT_MUTED

    # 3 Stat / Metric Highlight Cards
    card_w = Inches(3.644)
    card_h = Inches(2.4)
    top_pos = Inches(4.3)

    # Stat 1: Throughput
    add_card(slide1, Inches(0.8), top_pos, card_w, card_h, border_color=COLOR_CYAN)
    sb1 = slide1.shapes.add_textbox(Inches(1.0), top_pos + Inches(0.2), card_w - Inches(0.4), card_h - Inches(0.4))
    tf_sb1 = sb1.text_frame
    tf_sb1.word_wrap = True
    p = tf_sb1.paragraphs[0]
    p.text = "HIGH THROUGHPUT"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(6)
    p = tf_sb1.add_paragraph()
    p.text = "1M+ req/sec"
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(6)
    p = tf_sb1.add_paragraph()
    p.text = "Zero-copy network transfer & sequential append-only disk log I/O."
    p.font.size = Pt(12)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Stat 2: Fault Tolerance
    add_card(slide1, Inches(4.844), top_pos, card_w, card_h, border_color=COLOR_GREEN)
    sb2 = slide1.shapes.add_textbox(Inches(5.044), top_pos + Inches(0.2), card_w - Inches(0.4), card_h - Inches(0.4))
    tf_sb2 = sb2.text_frame
    tf_sb2.word_wrap = True
    p = tf_sb2.paragraphs[0]
    p.text = "HIGH AVAILABILITY"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(6)
    p = tf_sb2.add_paragraph()
    p.text = "Zero Data Loss"
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(6)
    p = tf_sb2.add_paragraph()
    p.text = "In-Sync Replicas (ISR), configurable Leader/Follower ACK guarantees."
    p.font.size = Pt(12)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Stat 3: Decoupling
    add_card(slide1, Inches(8.888), top_pos, card_w, card_h, border_color=COLOR_AMBER)
    sb3 = slide1.shapes.add_textbox(Inches(9.088), top_pos + Inches(0.2), card_w - Inches(0.4), card_h - Inches(0.4))
    tf_sb3 = sb3.text_frame
    tf_sb3.word_wrap = True
    p = tf_sb3.paragraphs[0]
    p.text = "DECOUPLED PIPELINES"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_AMBER
    p.space_after = Pt(6)
    p = tf_sb3.add_paragraph()
    p.text = "Pub/Sub Model"
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(6)
    p = tf_sb3.add_paragraph()
    p.text = "Scalable parallel consumer groups with pull-based streaming."
    p.font.size = Pt(12)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # =========================================================================
    # SLIDE 2: What is Apache Kafka? & Core Concept
    # =========================================================================
    slide2 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide2)
    add_header(slide2, "What is Apache Kafka? Paradigm Shift in Data Flow", "FOUNDATIONAL CONCEPT")

    col_w = Inches(5.666)
    col_h = Inches(5.4)

    # Left Card: Core Paradigm & Theory
    add_card(slide2, Inches(0.8), Inches(1.5), col_w, col_h, border_color=COLOR_ORANGE)
    tbox2_l = slide2.shapes.add_textbox(Inches(1.0), Inches(1.7), col_w - Inches(0.4), col_h - Inches(0.4))
    tf2_l = tbox2_l.text_frame
    tf2_l.word_wrap = True

    p = tf2_l.paragraphs[0]
    p.text = "CORE ARCHITECTURAL PARADIGM"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(14)

    add_bullet_point(tf2_l, "Distributed Commit Log", "Kafka is fundamentally an append-only, immutable distributed commit log stored sequentially on disk across a cluster of brokers.", COLOR_ORANGE)
    add_bullet_point(tf2_l, "Event Streaming vs Queues", "Traditional queues delete messages after consumption. Kafka retains events durably for a configurable period (e.g. 7 days or infinite).", COLOR_CYAN)
    add_bullet_point(tf2_l, "Pull-Based Consumption", "Consumers pull records at their own speed. Slow consumers won't get overwhelmed or crash upstream producers.", COLOR_GREEN)
    add_bullet_point(tf2_l, "High Scale & Replayability", "Events can be re-processed from any historical offset, enabling time-travel data backfills and analytics.", COLOR_AMBER)

    # Right Card: Real-Time Example & Coupling Comparison
    add_card(slide2, Inches(6.866), Inches(1.5), col_w, col_h, border_color=COLOR_CYAN)
    tbox2_r = slide2.shapes.add_textbox(Inches(7.066), Inches(1.7), col_w - Inches(0.4), col_h - Inches(0.4))
    tf2_r = tbox2_r.text_frame
    tf2_r.word_wrap = True

    p = tf2_r.paragraphs[0]
    p.text = "REAL-TIME EXAMPLE & DECOUPLING"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(14)

    add_bullet_point(tf2_r, "Real-Time Example", "E-Commerce Order Flow (Amazon / Shopify). When a user buys an item, an 'Order Placed' event is emitted once to Kafka.", COLOR_CYAN)
    add_bullet_point(tf2_r, "Parallel Microservices", "Payment Service, Inventory System, Shipping Pipeline, and Fraud Engine all consume the single event concurrently.", COLOR_GREEN)

    # Sub-card inside Right Card for Architectural Contrast
    sub_box = slide2.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(4.3), Inches(5.2), Inches(2.3))
    sub_box.fill.solid()
    sub_box.fill.fore_color.rgb = COLOR_CARD_HEADER
    sub_box.line.color.rgb = COLOR_CARD_BORDER
    tf_sub = sub_box.text_frame
    tf_sub.word_wrap = True
    tf_sub.margin_left = tf_sub.margin_right = Inches(0.15)
    tf_sub.margin_top = Inches(0.15)

    p = tf_sub.paragraphs[0]
    p.text = "Legacy Direct API vs Kafka Mesh"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(6)

    p = tf_sub.add_paragraph()
    p.text = "• Legacy (Monolithic): N x M direct REST calls create tight coupling and cascading failures."
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_MUTED
    p.space_after = Pt(4)

    p = tf_sub.add_paragraph()
    p.text = "• Kafka Mesh: Single source of event truth. Producers don't know who consumers are!"
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_GREEN

    # =========================================================================
    # SLIDE 3: High-Level Kafka Architecture Overview & Workflow
    # =========================================================================
    slide3 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide3)
    add_header(slide3, "High-Level Architecture & Component Workflow", "SYSTEM COMPONENT OVERVIEW")

    # Top Flowchart Container
    add_card(slide3, Inches(0.8), Inches(1.5), Inches(11.733), Inches(2.2), border_color=COLOR_CYAN)
    
    # Flowchart Nodes (Producers -> Kafka Cluster -> Consumers)
    node_w = Inches(3.2)
    node_h = Inches(1.5)
    
    # Node 1: Producers
    add_card(slide3, Inches(1.1), Inches(1.85), node_w, node_h, bg_color=COLOR_CARD_HEADER, border_color=COLOR_ORANGE)
    tb = slide3.shapes.add_textbox(Inches(1.1), Inches(1.85), node_w, node_h)
    tf_n = tb.text_frame
    tf_n.word_wrap = True
    p = tf_n.paragraphs[0]
    p.text = "PRODUCERS"
    p.alignment = PP_ALIGN.CENTER
    p.font.bold = True
    p.font.size = Pt(14)
    p.font.color.rgb = COLOR_ORANGE
    p = tf_n.add_paragraph()
    p.text = "Web Apps, Microservices,\nIoT Devices, Database CDC"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_DIM

    # Arrow 1 -> 2
    tb_a1 = slide3.shapes.add_textbox(Inches(4.3), Inches(2.2), Inches(0.7), Inches(0.8))
    p = tb_a1.text_frame.paragraphs[0]
    p.text = "➔"
    p.font.size = Pt(28)
    p.font.color.rgb = COLOR_CYAN
    p.alignment = PP_ALIGN.CENTER

    # Node 2: Kafka Cluster
    add_card(slide3, Inches(5.066), Inches(1.85), node_w, node_h, bg_color=COLOR_CARD_HEADER, border_color=COLOR_CYAN)
    tb = slide3.shapes.add_textbox(Inches(5.066), Inches(1.85), node_w, node_h)
    tf_n = tb.text_frame
    tf_n.word_wrap = True
    p = tf_n.paragraphs[0]
    p.text = "KAFKA CLUSTER"
    p.alignment = PP_ALIGN.CENTER
    p.font.bold = True
    p.font.size = Pt(14)
    p.font.color.rgb = COLOR_CYAN
    p = tf_n.add_paragraph()
    p.text = "Broker 1  |  Broker 2  |  Broker 3\n[ KRaft Consensus Quorum ]"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_DIM

    # Arrow 2 -> 3
    tb_a2 = slide3.shapes.add_textbox(Inches(8.266), Inches(2.2), Inches(0.7), Inches(0.8))
    p = tb_a2.text_frame.paragraphs[0]
    p.text = "➔"
    p.font.size = Pt(28)
    p.font.color.rgb = COLOR_CYAN
    p.alignment = PP_ALIGN.CENTER

    # Node 3: Consumer Groups
    add_card(slide3, Inches(9.033), Inches(1.85), node_w, node_h, bg_color=COLOR_CARD_HEADER, border_color=COLOR_GREEN)
    tb = slide3.shapes.add_textbox(Inches(9.033), Inches(1.85), node_w, node_h)
    tf_n = tb.text_frame
    tf_n.word_wrap = True
    p = tf_n.paragraphs[0]
    p.text = "CONSUMER GROUPS"
    p.alignment = PP_ALIGN.CENTER
    p.font.bold = True
    p.font.size = Pt(14)
    p.font.color.rgb = COLOR_GREEN
    p = tf_n.add_paragraph()
    p.text = "Analytics Engines, ML Pipelines,\nElasticSearch, Data Warehouses"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_DIM

    # Bottom 3 Detail Column Cards
    col3_w = Inches(3.644)
    col3_h = Inches(3.0)
    top3_pos = Inches(4.0)

    # Detail 1: Producer
    add_card(slide3, Inches(0.8), top3_pos, col3_w, col3_h, border_color=COLOR_ORANGE)
    tb_d1 = slide3.shapes.add_textbox(Inches(1.0), top3_pos + Inches(0.2), col3_w - Inches(0.4), col3_h - Inches(0.4))
    tf_d1 = tb_d1.text_frame
    tf_d1.word_wrap = True
    p = tf_d1.paragraphs[0]
    p.text = "1. PRODUCER ROLE"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(8)
    add_bullet_point(tf_d1, "Push Model", "Pushes record batches directly to partition leader brokers.", COLOR_ORANGE, 6)
    add_bullet_point(tf_d1, "Routing", "Uses key hash or round-robin for partition routing.", COLOR_TEXT_DIM, 6)

    # Detail 2: Broker Cluster
    add_card(slide3, Inches(4.844), top3_pos, col3_w, col3_h, border_color=COLOR_CYAN)
    tb_d2 = slide3.shapes.add_textbox(Inches(5.044), top3_pos + Inches(0.2), col3_w - Inches(0.4), col3_h - Inches(0.4))
    tf_d2 = tb_d2.text_frame
    tf_d2.word_wrap = True
    p = tf_d2.paragraphs[0]
    p.text = "2. BROKER CLUSTER"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(8)
    add_bullet_point(tf_d2, "Storage Node", "Stores log segments on disk and serves client requests.", COLOR_CYAN, 6)
    add_bullet_point(tf_d2, "Replication", "Maintains leader/follower partition replicas for HA.", COLOR_TEXT_DIM, 6)

    # Detail 3: Consumer
    add_card(slide3, Inches(8.888), top3_pos, col3_w, col3_h, border_color=COLOR_GREEN)
    tb_d3 = slide3.shapes.add_textbox(Inches(9.088), top3_pos + Inches(0.2), col3_w - Inches(0.4), col3_h - Inches(0.4))
    tf_d3 = tb_d3.text_frame
    tf_d3.word_wrap = True
    p = tf_d3.paragraphs[0]
    p.text = "3. CONSUMER ROLE"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(8)
    add_bullet_point(tf_d3, "Pull Model", "Pulls event batches asynchronously using offsets.", COLOR_GREEN, 6)
    add_bullet_point(tf_d3, "Group Scale", "Coordinates partition ownership within group.", COLOR_TEXT_DIM, 6)

    # =========================================================================
    # SLIDE 4: Topics, Partitions & Offsets
    # =========================================================================
    slide4 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide4)
    add_header(slide4, "Topics, Partitions & Monotonic Offsets", "CORE DATA ABSTRACTION")

    # Left Card: Theory & Definitions
    add_card(slide4, Inches(0.8), Inches(1.5), Inches(5.5), Inches(5.4), border_color=COLOR_AMBER)
    tb4_l = slide4.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.1), Inches(5.0))
    tf4_l = tb4_l.text_frame
    tf4_l.word_wrap = True

    p = tf4_l.paragraphs[0]
    p.text = "DEFINITIONS & ORDERING RULES"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_AMBER
    p.space_after = Pt(12)

    add_bullet_point(tf4_l, "Topic", "Logical stream name (e.g. 'order-events' or 'sensor-telemetry'). Analogy: A database table name.", COLOR_AMBER, 10)
    add_bullet_point(tf4_l, "Partition", "A topic is split into physical partitions distributed across cluster brokers for throughput.", COLOR_CYAN, 10)
    add_bullet_point(tf4_l, "Offset", "A unique, incremental 64-bit integer ID assigned to every message written to a partition.", COLOR_GREEN, 10)
    add_bullet_point(tf4_l, "CRITICAL ORDERING RULE", "Ordering is guaranteed STRICTLY WITHIN A PARTITION. Messages across different partitions are not globally ordered!", COLOR_ORANGE, 10)

    # Right Card: Visual Partition Array Diagram
    add_card(slide4, Inches(6.6), Inches(1.5), Inches(5.933), Inches(5.4), border_color=COLOR_CYAN)
    tb4_r = slide4.shapes.add_textbox(Inches(6.8), Inches(1.7), Inches(5.533), Inches(5.0))
    tf4_r = tb4_r.text_frame
    tf4_r.word_wrap = True

    p = tf4_r.paragraphs[0]
    p.text = "TOPIC: 'order-events' (PARTITION LAYOUT)"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(14)

    # Render Visual Partition Arrays
    def render_partition_visual(slide, top_y, label, offsets, consumer_pos=None):
        # Label Box
        tb_p = slide.shapes.add_textbox(Inches(6.8), top_y, Inches(1.8), Inches(0.4))
        p = tb_p.text_frame.paragraphs[0]
        p.text = label
        p.font.size = Pt(11)
        p.font.bold = True
        p.font.color.rgb = COLOR_TEXT_WHITE
        
        # Offset cells
        start_x = 8.5
        cell_w = 0.7
        for idx, val in enumerate(offsets):
            cell = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(start_x + idx * 0.75), top_y, Inches(0.68), Inches(0.55))
            cell.fill.solid()
            if idx == consumer_pos:
                cell.fill.fore_color.rgb = COLOR_GREEN
                cell.line.color.rgb = COLOR_TEXT_WHITE
            else:
                cell.fill.fore_color.rgb = COLOR_CARD_HEADER
                cell.line.color.rgb = COLOR_CARD_BORDER
            
            p = cell.text_frame.paragraphs[0]
            p.text = f"Offset\n{val}"
            p.alignment = PP_ALIGN.CENTER
            p.font.size = Pt(9)
            p.font.color.rgb = COLOR_TEXT_WHITE

    render_partition_visual(slide4, Inches(2.4), "Partition 0", [0, 1, 2, 3, 4], consumer_pos=4)
    render_partition_visual(slide4, Inches(3.6), "Partition 1", [0, 1, 2, 3], consumer_pos=2)
    render_partition_visual(slide4, Inches(4.8), "Partition 2", [0, 1, 2, 3, 4, 5], consumer_pos=5)

    # Legend at bottom of right card
    leg_box = slide4.shapes.add_textbox(Inches(6.8), Inches(5.8), Inches(5.533), Inches(0.8))
    tf_leg = leg_box.text_frame
    tf_leg.word_wrap = True
    p = tf_leg.paragraphs[0]
    p.text = "■ Green Cell = Current Consumer Read Offset Pointer"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p = tf_leg.add_paragraph()
    p.text = "New records are continuously appended to the tail of each partition."
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # =========================================================================
    # SLIDE 5: Commit Log & Zero-Copy Architecture
    # =========================================================================
    slide5 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide5)
    add_header(slide5, "Why Kafka is Fast: Commit Log & Zero-Copy I/O", "STORAGE & PERFORMANCE INTERNALS")

    # Left Card: Sequential Disk I/O & Page Cache
    add_card(slide5, Inches(0.8), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_GREEN)
    tb5_l = slide5.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.266), Inches(5.0))
    tf5_l = tb5_l.text_frame
    tf5_l.word_wrap = True

    p = tf5_l.paragraphs[0]
    p.text = "1. SEQUENTIAL DISK I/O & PAGE CACHE"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(14)

    add_bullet_point(tf5_l, "Sequential vs Random I/O", "Sequential disk writes operate at ~600 MB/s, matching RAM speed because disk heads don't need physical seek movement.", COLOR_GREEN, 10)
    add_bullet_point(tf5_l, "Linux OS Page Cache", "Kafka writes records directly to the OS Page Cache. Data stays cached in RAM without JVM GC overhead.", COLOR_CYAN, 10)
    add_bullet_point(tf5_l, "Log Segment Files", "Partitions are stored on disk as immutable segment files (.log) with binary index files (.index) for O(1) offset lookup.", COLOR_AMBER, 10)

    # Right Card: Zero-Copy Network Transfer Comparison
    add_card(slide5, Inches(6.866), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_ORANGE)
    tb5_r = slide5.shapes.add_textbox(Inches(7.066), Inches(1.7), Inches(5.266), Inches(5.0))
    tf5_r = tb5_r.text_frame
    tf5_r.word_wrap = True

    p = tf5_r.paragraphs[0]
    p.text = "2. ZERO-COPY NETWORK TRANSFER (sendfile)"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(10)

    # Comparison Boxes
    box_trad = slide5.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(2.3), Inches(5.2), Inches(2.0))
    box_trad.fill.solid()
    box_trad.fill.fore_color.rgb = COLOR_CARD_HEADER
    box_trad.line.color.rgb = COLOR_CARD_BORDER
    tf_tr = box_trad.text_frame
    tf_tr.word_wrap = True
    tf_tr.margin_left = tf_tr.margin_top = Inches(0.12)

    p = tf_tr.paragraphs[0]
    p.text = "TRADITIONAL TRANSFER (4 Copies, 4 Context Switches)"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_MUTED
    p.space_after = Pt(4)

    p = tf_tr.add_paragraph()
    p.text = "Disk ➔ OS Page Cache ➔ App User Buffer ➔ Socket Buffer ➔ NIC Buffer"
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_DIM
    p.space_after = Pt(4)
    p = tf_tr.add_paragraph()
    p.text = "• Massive CPU context switching & RAM copying overhead."
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_AMBER

    box_zero = slide5.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(4.5), Inches(5.2), Inches(2.1))
    box_zero.fill.solid()
    box_zero.fill.fore_color.rgb = COLOR_CARD_HEADER
    box_zero.line.color.rgb = COLOR_ORANGE
    tf_zr = box_zero.text_frame
    tf_zr.word_wrap = True
    tf_zr.margin_left = tf_zr.margin_top = Inches(0.12)

    p = tf_zr.paragraphs[0]
    p.text = "KAFKA ZERO-COPY (sendfile system call)"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(4)

    p = tf_zr.add_paragraph()
    p.text = "Disk ➔ OS Page Cache ------------------------➔ NIC Buffer"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(4)
    p = tf_zr.add_paragraph()
    p.text = "• Bypasses user space entirely! Data is copied directly by DMA engine, saturating network links at line speed."
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_DIM

    # =========================================================================
    # SLIDE 6: Producers & Partitioning Strategies
    # =========================================================================
    slide6 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide6)
    add_header(slide6, "Producer Routing, Batching & Real-Time Examples", "PRODUCER ARCHITECTURE")

    # Left Card: Partitioning Strategies
    add_card(slide6, Inches(0.8), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_CYAN)
    tb6_l = slide6.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.266), Inches(5.0))
    tf6_l = tb6_l.text_frame
    tf6_l.word_wrap = True

    p = tf6_l.paragraphs[0]
    p.text = "PARTITION ROUTING STRATEGIES"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(14)

    add_bullet_point(tf6_l, "1. Key-Based Hashing", "MurmurHash2(Key) % NumPartitions. Guarantees that all events with the same key land on the EXACT SAME partition.", COLOR_CYAN, 12)
    add_bullet_point(tf6_l, "2. Sticky Partitioner", "When no key is specified, Kafka batches records into a single partition until full, maximizing compression & TCP packet efficiency.", COLOR_GREEN, 12)
    add_bullet_point(tf6_l, "3. Custom Partitioner", "Implement custom business logic (e.g. routing high-priority VIP customer events to dedicated partitions).", COLOR_AMBER, 12)

    # Right Card: Real-Time Example (Uber GPS Telemetry)
    add_card(slide6, Inches(6.866), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_GREEN)
    tb6_r = slide6.shapes.add_textbox(Inches(7.066), Inches(1.7), Inches(5.266), Inches(5.0))
    tf6_r = tb6_r.text_frame
    tf6_r.word_wrap = True

    p = tf6_r.paragraphs[0]
    p.text = "REAL-TIME EXAMPLE: UBER DRIVER TELEMETRY"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(14)

    add_bullet_point(tf6_r, "Scenario", "Millions of drivers stream GPS location updates (lat, long, speed) every 2 seconds.", COLOR_GREEN, 8)
    add_bullet_point(tf6_r, "Partition Key Strategy", "Set Record Key = 'driver_id' (e.g. driver_8841).", COLOR_CYAN, 8)

    # Workflow visual inside example card
    box_ex = slide6.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(3.8), Inches(5.2), Inches(2.7))
    box_ex.fill.solid()
    box_ex.fill.fore_color.rgb = COLOR_CARD_HEADER
    box_ex.line.color.rgb = COLOR_CARD_BORDER
    tf_ex = box_ex.text_frame
    tf_ex.word_wrap = True
    tf_ex.margin_left = tf_ex.margin_top = Inches(0.15)

    p = tf_ex.paragraphs[0]
    p.text = "Why Key Partitioning Matters Here:"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(6)

    p = tf_ex.add_paragraph()
    p.text = "1. Driver 8841 updates land exclusively in Partition 2."
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_DIM
    p.space_after = Pt(4)

    p = tf_ex.add_paragraph()
    p.text = "2. Chronological sequence is preserved 100% in order."
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(4)

    p = tf_ex.add_paragraph()
    p.text = "3. Downstream ETA & fare calculation engines receive accurate location trajectory without out-of-order jitter!"
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # =========================================================================
    # SLIDE 7: Kafka Brokers & Consensus (ZooKeeper vs KRaft)
    # =========================================================================
    slide7 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide7)
    add_header(slide7, "Broker Cluster & Consensus: ZooKeeper to KRaft", "CLUSTER MANAGEMENT & CONSENSUS")

    # Top Half: ZooKeeper vs KRaft Comparison (2 Side-by-Side Cards)
    card_w7 = Inches(5.666)
    card_h7 = Inches(3.1)

    # Card 1: Legacy ZooKeeper
    add_card(slide7, Inches(0.8), Inches(1.5), card_w7, card_h7, border_color=COLOR_AMBER)
    tb7_1 = slide7.shapes.add_textbox(Inches(1.0), Inches(1.65), card_w7 - Inches(0.4), card_h7 - Inches(0.3))
    tf7_1 = tb7_1.text_frame
    tf7_1.word_wrap = True
    p = tf7_1.paragraphs[0]
    p.text = "LEGACY ARCHITECTURE: ZOOKEEPER"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_AMBER
    p.space_after = Pt(8)
    add_bullet_point(tf7_1, "External Ensemble", "Requires separate 3 or 5 node ZooKeeper cluster for metadata.", COLOR_AMBER, 6)
    add_bullet_point(tf7_1, "Scale Limit", "Metadata sync bottleneck when scaling past 200,000 partitions.", COLOR_TEXT_DIM, 6)

    # Card 2: KRaft Modern Consensus
    add_card(slide7, Inches(6.866), Inches(1.5), card_w7, card_h7, border_color=COLOR_GREEN)
    tb7_2 = slide7.shapes.add_textbox(Inches(7.066), Inches(1.65), card_w7 - Inches(0.4), card_h7 - Inches(0.3))
    tf7_2 = tb7_2.text_frame
    tf7_2.word_wrap = True
    p = tf7_2.paragraphs[0]
    p.text = "MODERN ARCHITECTURE: KRAFT (KIP-500)"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(8)
    add_bullet_point(tf7_2, "Internal Metadata Quorum", "Event-based Raft consensus runs directly inside Kafka brokers.", COLOR_GREEN, 6)
    add_bullet_point(tf7_2, "Sub-Second Failover", "Supports 1 Million+ partitions with instant controller recovery.", COLOR_CYAN, 6)

    # Bottom Card: Broker Node Core Responsibilities
    add_card(slide7, Inches(0.8), Inches(4.8), Inches(11.733), Inches(2.1), border_color=COLOR_CYAN)
    tb7_3 = slide7.shapes.add_textbox(Inches(1.0), Inches(4.95), Inches(11.333), Inches(1.8))
    tf7_3 = tb7_3.text_frame
    tf7_3.word_wrap = True

    p = tf7_3.paragraphs[0]
    p.text = "BROKER NODE CORE RESPONSIBILITIES"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(8)

    add_bullet_point(tf7_3, "Network I/O & Client Requests", "Handles Produce, Fetch, and Metadata requests from clients.", COLOR_CYAN, 6)
    add_bullet_point(tf7_3, "Retention Policies", "Executes automated log cleanup based on retention time (e.g. 7 days) or topic size limits (e.g. 50GB).", COLOR_TEXT_DIM, 6)
    add_bullet_point(tf7_3, "Log Compaction", "Retains only the latest message value for each key, ideal for state restoration tables.", COLOR_GREEN, 6)

    # =========================================================================
    # SLIDE 8: Data Replication & Fault Tolerance (HA & ISR)
    # =========================================================================
    slide8 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide8)
    add_header(slide8, "Replication Factor, In-Sync Replicas & ACKS", "HIGH AVAILABILITY & DURABILITY")

    # Left Card: Leader vs Followers & ISR
    add_card(slide8, Inches(0.8), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_GREEN)
    tb8_l = slide8.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.266), Inches(5.0))
    tf8_l = tb8_l.text_frame
    tf8_l.word_wrap = True

    p = tf8_l.paragraphs[0]
    p.text = "REPLICATION & LEADER/FOLLOWER MODEL"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(14)

    add_bullet_point(tf8_l, "Leader Replica", "One broker acts as Leader for a partition. All client writes and reads go through the Leader.", COLOR_GREEN, 10)
    add_bullet_point(tf8_l, "Follower Replicas", "Followers passively fetch log records from the Leader to replicate data across nodes.", COLOR_CYAN, 10)
    add_bullet_point(tf8_l, "In-Sync Replicas (ISR)", "Set of followers actively caught up with the Leader's high watermark within replica.lag.time.max.ms.", COLOR_AMBER, 10)
    add_bullet_point(tf8_l, "Automatic Leader Election", "If Leader broker fails, KRaft instantly promotes an ISR follower to be the new Leader.", COLOR_ORANGE, 10)

    # Right Card: Producer ACKS & Durability Guarantees
    add_card(slide8, Inches(6.866), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_ORANGE)
    tb8_r = slide8.shapes.add_textbox(Inches(7.066), Inches(1.7), Inches(5.266), Inches(5.0))
    tf8_r = tb8_r.text_frame
    tf8_r.word_wrap = True

    p = tf8_r.paragraphs[0]
    p.text = "PRODUCER ACKS & DURABILITY LEVELS"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(12)

    add_bullet_point(tf8_r, "acks = 0 (Fire & Forget)", "Producer doesn't wait for acknowledgement. Max throughput, high data loss risk.", COLOR_TEXT_MUTED, 10)
    add_bullet_point(tf8_r, "acks = 1 (Leader Ack)", "Producer waits for Leader to write record to local log. Balanced throughput.", COLOR_CYAN, 10)
    add_bullet_point(tf8_r, "acks = all / -1 (ISR Ack)", "Producer waits for Leader AND all ISR followers to write. ZERO DATA LOSS guarantee!", COLOR_GREEN, 10)

    # Metric box at bottom right
    box_m = slide8.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(5.1), Inches(5.2), Inches(1.5))
    box_m.fill.solid()
    box_m.fill.fore_color.rgb = COLOR_CARD_HEADER
    box_m.line.color.rgb = COLOR_GREEN
    tf_m = box_m.text_frame
    tf_m.word_wrap = True
    tf_m.margin_left = tf_m.margin_top = Inches(0.12)

    p = tf_m.paragraphs[0]
    p.text = "Recommended Enterprise HA Config:"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(4)

    p = tf_m.add_paragraph()
    p.text = "Replication Factor = 3  |  min.insync.replicas = 2  |  acks = all"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(2)

    p = tf_m.add_paragraph()
    p.text = "Tolerates up to 2 broker failures without data loss!"
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # =========================================================================
    # SLIDE 9: Consumer Groups, Rebalancing & Parallel Scale-Out
    # =========================================================================
    slide9 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide9)
    add_header(slide9, "Consumer Groups, Parallelism & Rebalancing", "CONSUMER ARCHITECTURE")

    # Left Card: Consumer Group Rules
    add_card(slide9, Inches(0.8), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_AMBER)
    tb9_l = slide9.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.266), Inches(5.0))
    tf9_l = tb9_l.text_frame
    tf9_l.word_wrap = True

    p = tf9_l.paragraphs[0]
    p.text = "PARALLEL CONSUMPTION & SCALING LAWS"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_AMBER
    p.space_after = Pt(14)

    add_bullet_point(tf9_l, "Consumer Group Pattern", "A group of worker processes sharing partition workload for scalable consumption.", COLOR_AMBER, 10)
    add_bullet_point(tf9_l, "1 Partition = 1 Consumer", "Each partition is assigned to AT MOST one consumer inside a group to prevent duplicate reads.", COLOR_CYAN, 10)
    add_bullet_point(tf9_l, "Scaling Rule", "Max active consumers in a group = Total number of partitions. Excess consumers sit idle as backups!", COLOR_GREEN, 10)
    add_bullet_point(tf9_l, "Offset Commits", "Consumers commit processed offsets (__consumer_offsets topic) enabling At-Least-Once or Exactly-Once semantics.", COLOR_ORANGE, 10)

    # Right Card: Rebalancing & Real-Time Financial Example
    add_card(slide9, Inches(6.866), Inches(1.5), Inches(5.666), Inches(5.4), border_color=COLOR_CYAN)
    tb9_r = slide9.shapes.add_textbox(Inches(7.066), Inches(1.7), Inches(5.266), Inches(5.0))
    tf9_r = tb9_r.text_frame
    tf9_r.word_wrap = True

    p = tf9_r.paragraphs[0]
    p.text = "REBALANCING & REAL-TIME BANKING EXAMPLE"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(14)

    add_bullet_point(tf9_r, "Group Coordinator & Rebalancing", "When a consumer node fails or joins, the broker coordinator redistributes partition assignments automatically.", COLOR_CYAN, 10)
    add_bullet_point(tf9_r, "Real-Time Example", "Financial Fraud Detection Cluster", COLOR_GREEN, 8)

    box_cg = slide9.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(3.9), Inches(5.2), Inches(2.6))
    box_cg.fill.solid()
    box_cg.fill.fore_color.rgb = COLOR_CARD_HEADER
    box_cg.line.color.rgb = COLOR_CARD_BORDER
    tf_cg = box_cg.text_frame
    tf_cg.word_wrap = True
    tf_cg.margin_left = tf_cg.margin_top = Inches(0.12)

    p = tf_cg.paragraphs[0]
    p.text = "Banking Stream (8 Partitions):"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(4)

    p = tf_cg.add_paragraph()
    p.text = "• 4 Fraud Consumer Worker Instances in Group."
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_DIM
    p.space_after = Pt(2)

    p = tf_cg.add_paragraph()
    p.text = "• Allocation: Each Worker inspects 2 Partitions in parallel."
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(2)

    p = tf_cg.add_paragraph()
    p.text = "• Result: High-volume credit card swipes analyzed for fraud under 10 milliseconds across all partitions!"
    p.font.size = Pt(10)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # =========================================================================
    # SLIDE 10: Conclusion & Architecture Recap (Thank You)
    # =========================================================================
    slide10 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide10)
    add_header(slide10, "Architecture Summary & Enterprise Impact", "SUMMARY & CONCLUSION")

    # Left Summary Card (5 Pillars)
    add_card(slide10, Inches(0.8), Inches(1.5), Inches(6.0), Inches(5.4), border_color=COLOR_ORANGE)
    tb10_l = slide10.shapes.add_textbox(Inches(1.0), Inches(1.7), Inches(5.6), Inches(5.0))
    tf10_l = tb10_l.text_frame
    tf10_l.word_wrap = True

    p = tf10_l.paragraphs[0]
    p.text = "5 KAFKA ARCHITECTURAL PILLARS"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = COLOR_ORANGE
    p.space_after = Pt(12)

    add_bullet_point(tf10_l, "1. Append-Only Log", "Immutable, sequential disk storage yielding extreme I/O speed.", COLOR_ORANGE, 8)
    add_bullet_point(tf10_l, "2. Zero-Copy I/O", "Kernel Page Cache to NIC DMA bypasses CPU & memory context switches.", COLOR_CYAN, 8)
    add_bullet_point(tf10_l, "3. Partition Scale", "Horizontal linear scalability for parallel producers & consumer groups.", COLOR_GREEN, 8)
    add_bullet_point(tf10_l, "4. KRaft Consensus", "Native internal Raft metadata log eliminating ZooKeeper dependencies.", COLOR_AMBER, 8)
    add_bullet_point(tf10_l, "5. ISR Replication", "Primary-backup leader/follower model delivering zero data loss HA.", COLOR_PURPLE, 8)

    # Right Thank You & Q&A Card
    add_card(slide10, Inches(7.2), Inches(1.5), Inches(5.333), Inches(5.4), bg_color=COLOR_CARD_HEADER, border_color=COLOR_GREEN)
    tb10_r = slide10.shapes.add_textbox(Inches(7.4), Inches(2.2), Inches(4.933), Inches(4.0))
    tf10_r = tb10_r.text_frame
    tf10_r.word_wrap = True

    p = tf10_r.paragraphs[0]
    p.text = "Thank You!"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(40)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEXT_WHITE
    p.space_after = Pt(10)

    p = tf10_r.add_paragraph()
    p.text = "Questions & Architectural Discussion"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = COLOR_GREEN
    p.space_after = Pt(20)

    box_th = slide10.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.6), Inches(4.2), Inches(4.533), Inches(2.0))
    box_th.fill.solid()
    box_th.fill.fore_color.rgb = COLOR_CARD_BG
    box_th.line.color.rgb = COLOR_CARD_BORDER
    tf_th = box_th.text_frame
    tf_th.word_wrap = True
    tf_th.margin_top = Inches(0.2)

    p = tf_th.paragraphs[0]
    p.text = "Enterprise Event Streaming"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = COLOR_CYAN
    p.space_after = Pt(6)

    p = tf_th.add_paragraph()
    p.text = "Adopted by 80%+ of Fortune 100 Companies\n(LinkedIn, Netflix, Uber, PayPal, Goldman Sachs)"
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(11)
    p.font.color.rgb = COLOR_TEXT_MUTED

    # Save presentation
    output_path = "Apache_Kafka_Architecture.pptx"
    prs.save(output_path)
    print(f"Successfully generated presentation: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    create_kafka_presentation()
