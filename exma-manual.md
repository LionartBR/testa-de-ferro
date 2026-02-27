**ExMA**

Explicit Modular Architecture

_Development Manual for the Era of Human-AI Collaboration_

Version 1.0

January 2026

# Table of Contents

# Part I: Foundations

## What is ExMA?

ExMA (Explicit Modular Architecture) is a software architecture designed specifically to maximize the effectiveness of collaboration between human developers and AI assistants. It synthesizes decades of research in code comprehension, software design, and complexity management under a new lens: optimization for limited context and explicit reasoning.

The architecture does not invent new concepts — it integrates established principles coherently, with a unifying purpose: creating code that both humans and AIs can navigate, understand, and modify with precision.

## Why ExMA?

Recent research demonstrates that LLMs suffer dramatic performance degradation as context increases. A 2025 benchmark showed Claude 3.5 Sonnet dropping from 29% to 3% accuracy with growing contexts. Binary tasks like bug fixing amplify errors: small mistakes cascade into complete failure.

Paradoxically, the same practices that improve human code comprehension also improve comprehension by LLMs. Investing in ExMA yields a double return: more maintainable code for humans and more effective AI assistance.

## Fundamental Principle: Maximum Locality

_If the AI needs external context to understand a code snippet, that snippet is poorly designed._

AI works best when everything it needs is nearby and visible. Every architectural decision should favor code that can be understood without jumping across dozens of files, without tracing deep inheritance hierarchies, without depending on implicit conventions.

This principle guides all the others. When in doubt about a design decision, ask: does this increase or decrease the locality of the code?

# Part II: The Ten Pillars

## 1. Vertical Slice Architecture

**What:** Organize code by feature or use case, not by technical layer.

**Why:** The AI can load an entire feature into context and understand the complete flow, from input to output.

### Traditional Structure (avoid)

```
src/controllers/chapter_controller.rs
src/services/chapter_service.rs
src/repositories/chapter_repository.rs
```

### Vertical Structure (prefer)

```
src/features/add_chapter/mod.rs
src/features/add_chapter/command.rs
src/features/add_chapter/handler.rs
src/features/add_chapter/tests.rs
```

### Documented Benefits

- Higher cohesion: related code lives together
- Package-private scoping: visibility restricted to the module
- Deletion test: removing a feature = deleting a folder
- Simplified navigation for humans and AIs

### Practical Rule

If you need to open more than 3-4 files to understand a feature, refactor. The feature should be comprehensible as a unit.

## 2. Functional Core, Imperative Shell

**What:** Radically separate business logic (pure, no side effects) from infrastructure (IO, database, APIs).

**Why:** The pure core is the ideal environment for AI — it can generate, test, and refactor without mocks, without setup, without concern for external state.

### The Impureim Sandwich Pattern

Mark Seemann demonstrated that well-structured code follows the pattern: impure → pure → impure. Read input (impure), process (pure), write output (impure).

### Directory Structure

```
src/core/          # Pure: data transformations only
src/core/manuscript.rs
src/core/chapter.rs
src/shell/         # Impure: IO, persistence, APIs
src/shell/persistence/
src/shell/api/
```

### Characteristics of the Pure Core

- Deterministic functions: same input always produces same output
- No IO or external state dependencies
- Testable without mocks or complex setup
- Parallelizable by nature

### Practical Rule

If a domain function needs async or receives a repository as a parameter, it probably belongs in the shell, not the core.

## 3. Deep Modules

**What:** Favor modules with simple interfaces that hide substantial implementation.

**Why:** Deep modules keep related code together while presenting clear contracts. Shallow modules fragment logic and increase context jumps.

### John Ousterhout's Concept

In "A Philosophy of Software Design", Ousterhout introduces the depth metric: the ratio between functionality provided and interface complexity. Deep modules offer much through little.

### Complexity Symptoms to Avoid

- **Change amplification:** simple changes require modifications in many places
- **Cognitive load:** developers need to keep too much information in memory
- **Unknown unknowns:** it's not obvious what needs to be modified

### Practical Metrics

| Metric              | Recommended Limit | Reason                       |
| ------------------- | ----------------- | ---------------------------- |
| Cyclomatic complexity | < 10 per function | Limits execution paths       |
| Nesting depth       | < 4 levels        | Reduces cognitive load       |
| Line length         | < 120 characters  | Improves readability         |
| File size           | 200-400 lines     | Fits in the context window   |

### Practical Rule

"Pull complexity downward" — pull complexity into the modules instead of exposing it through interfaces. The AI should be able to use the module without understanding its implementation.

## 4. Types as Documentation

**What:** Use rich, specific types instead of generic primitives.

**Why:** Types are prompts embedded in the code. The AI infers intent, validations, and invariants automatically from the types.

### Comparison

Opaque signature (avoid):

```rust
fn process(text: String, start: usize, end: usize, flag: bool) -> String
```

Expressive signature (prefer):

```rust
fn extract_selection(content: &SceneContent, selection: TextRange, mode: ExtractionMode) -> SelectedText
```

### Types that Communicate

```rust
pub struct ChapterTitle(String);      // Not just any string
pub struct WordCount(u32);            // Not just any number
pub struct TextRange { start: Position, end: Position }

pub enum ExtractionMode {
    KeepFormatting,
    PlainText,
    MarkdownPreserving,
}

pub enum MergeError {
    IncompatibleStructure,
    ConflictingMetadata,
    EmptyChapter,
}
```

### Practical Rule

If you need a comment to explain what a parameter means, it should be a type. Wrong types don't compile; wrong comments pass silently.

## 5. Intent-Revealing Structure

**What:** The code structure should communicate the business domain, not the technical framework.

**Why:** When the structure screams the domain, both humans and AIs understand the purpose without reading the implementation.

### Screaming Architecture (Robert Martin)

When looking at the directory structure of a healthcare system, you should see: patients/, appointments/, prescriptions/ — not: controllers/, services/, repositories/.

### Naming Conventions that Communicate

- **Ports (Hexagonal):** ForPlacingOrders, ForStoringUsers — not OrderService, UserRepository
- **Events:** OrderPlaced, PaymentReceived — past tense verbs indicating facts
- **Commands:** PlaceOrder, ProcessPayment — imperatives indicating intent

### Code Beacons

Eye-tracking research shows that experienced programmers use "beacons" — recognizable patterns that activate mental schemas. Consistency in naming and structure creates beacons that accelerate comprehension.

### Practical Rule

A new developer (or an AI) should be able to describe what the system does by looking only at the directory structure and file names.

## 6. Small and Complete Files

**What:** Keep files between 200-400 lines. Each file should be comprehensible in isolation.

**Why:** Files that fit in the context window allow the AI to work with complete vision, not partial.

### Characteristics of a Good File

- **Cohesive:** deals with a single concept or responsibility
- **Self-contained:** explicit imports, no dependence on global context
- **Complete:** includes related types, implementation, and tests when small

### Signs that Splitting is Needed

- More than 400 lines
- Multiple impl blocks for different types
- Sections separated by comments like "// --- Helpers ---"
- Need to scroll to find related functions

### Lost-in-the-Middle Problem

Research shows that LLMs perform better when critical information appears at the beginning or end of context. Smaller files eliminate this problem; when necessary, place important information at the edges.

### Practical Rule

The AI should be able to read the entire file and say "I understand what this does" in one sentence.

## 7. Strategic Information Hiding

**What:** Modules should hide design decisions prone to change, not correspond to processing steps.

**Why:** Well-done encapsulation contains changes. Excessive encapsulation hides context that the AI needs.

### Parnas's Criterion (1972)

David Parnas established that modules should hide design decisions likely to change. This offers three benefits:

- **Managerial:** separate teams work independently
- **Flexibility:** drastic changes remain contained
- **Comprehensibility:** the system can be studied one module at a time

### Tension with AI

Aggressive information hiding can obscure context that AI tools need. The resolution: hide implementation details from other modules, but provide explicit interfaces with rich documentation for AI tools.

### Law of Demeter

"Only talk to your immediate friends." A method should only invoke methods on: its own object, received parameters, objects it creates, or direct component objects. This reduces hidden dependencies.

### Practical Rule

If swapping the implementation on one side of a boundary requires changes on the other side, the contract is leaking details.

## 8. ADRs and Inline Documentation

**What:** Document architectural decisions directly in the code, close to the affected code.

**Why:** The AI reads this and understands the why, not just the how. It prevents suggestions that break intentional decisions.

### Inline ADR Format

```rust
// ADR: We use Event Sourcing for Manuscript
//
// Context: We need complete history for:
//   - Manuscript versioning feature
//   - Writing pattern analysis
//   - Unlimited undo/redo
//
// Consequences:
//   - State is derived from events
//   - Queries may need projections
//   - Events are immutable
pub struct Manuscript {
    id: ManuscriptId,
    events: Vec<ManuscriptEvent>,
}
```

### Living Documentation (Cyrille Martraire)

Most of the knowledge worth sharing already exists in the system — in code, tests, version history. The focus should be on extracting and surfacing that knowledge, not creating separate documentation that inevitably diverges.

### What to Document

- Non-obvious design choices
- Consciously accepted trade-offs
- Project-specific patterns
- Reasons for NOT using a common approach

### Practical Rule

If you chose A over B for a specific reason, document it. The AI will eventually suggest B.

## 9. Tests as Specification

**What:** Write tests that function as executable documentation of requirements.

**Why:** The AI can generate tests as a way to understand requirements, and use existing tests as specification for implementations.

### Tests that Specify

```rust
#[cfg(test)]
mod merge_chapters_spec {
    #[test]
    fn preserves_scene_order_from_both_chapters() { }

    #[test]
    fn combines_metadata_preferring_first_chapter() { }

    #[test]
    fn fails_when_chapters_have_conflicting_timelines() { }

    #[test]
    fn generates_merge_event_for_history() { }
}
```

### Pattern for Test Names

- Describe behavior, not implementation
- Readable as natural language sentences
- Specify scenario and expected outcome
- Avoid technical terms when possible

### Practical Rule

Someone should be able to read only the test names and understand what the feature does. Tests are the living specification of behavior.

## 10. Explicit Contracts at Boundaries

**What:** Define clear interfaces/traits between modules. Never depend on concrete implementations across boundaries.

**Why:** The AI can implement one side of the contract without knowing the other. It facilitates isolated changes.

### Defining Contracts

```rust
// contracts/persistence.rs
pub trait ManuscriptRepository {
    async fn get(&self, id: ManuscriptId) -> Result<Manuscript, RepoError>;
    async fn save(&self, manuscript: Manuscript) -> Result<(), RepoError>;
    async fn list_by_author(&self, author: AuthorId) -> Result<Vec<Summary>, RepoError>;
}
```

### Dependency Rule (Clean Architecture)

Source code dependencies should point toward stability and abstraction. Inner modules (domain logic) should never reference outer modules (infrastructure).

### Typical Boundaries

- Core ↔ Persistence
- Core ↔ External APIs
- Core ↔ UI/CLI
- Feature ↔ Feature (when communication is necessary)

### Command-Query Separation

Bertrand Meyer established: every method should either change state (command) or return data (query), never both. This makes state changes visible at the signature level.

### Practical Rule

If swapping the implementation on one side of the boundary requires changes on the other side, the contract is leaking implementation details.

# Part III: Context Engineering for AI

## Context Optimization Principles

Anthropic's guidance (2025) for building AI agents reveals that context should be treated as a finite resource with diminishing returns.

### Minimal Viable Context

Find the smallest set of high-signal tokens that maximizes desired outcomes. More context is not always better — there is an inflection point where noise surpasses signal.

### Compaction Strategies

- Summarize conversation history when approaching context limits
- Preserve architectural decisions and unresolved issues
- Discard redundant outputs and already-processed implementation details

### Persistent Notes

For long-running tasks, maintain files like NOTES.md or ARCHITECTURE.md outside the context window but accessible. The AI can consult them when needed without always loading them.

### Sub-Agent Architecture

Specialized agents handle focused tasks with clean contexts, returning condensed summaries to lead agents. This maps well to vertical slices: one agent per feature.

## Strategic Context Redundancy

For AI optimization, repeat important information (module purpose, key constraints) in file-level documentation. Address the lost-in-the-middle problem by placing critical information at the edges of files.

### Standard File Header

```rust
//! # Chapter Management
//!
//! This module manages chapter structure within manuscripts.
//!
//! ## Invariants
//! - Chapters always belong to exactly one manuscript
//! - Scene order is preserved across all operations
//! - Chapter merges generate events for history
//!
//! ## Design Decisions
//! - Event sourcing to support undo/redo (see ADR-003)
```

# Part IV: Anti-Patterns

## What to Avoid

### Deep Inheritance

The AI needs to trace multiple classes to understand behavior. Each level adds required context. Prefer composition over inheritance.

### Magic and Convention over Configuration

Frameworks that "guess" behavior based on names or file locations confuse the AI. What seems convenient for experienced humans is opaque to AI assistants. Be explicit.

### Heavy Metaprogramming

Complex macros, reflection, runtime code generation are black boxes for the AI. It cannot trace what will be generated or executed. Use with extreme moderation.

### Implicit Global State

Singletons, global variables, implicit contexts break locality. The AI doesn't know what's available without scanning the entire codebase. Inject dependencies explicitly.

### God Objects

Files or structs that do everything violate the locality principle. The AI needs to load too much context to understand any part. Split into cohesive parts with clear responsibilities.

### Circular Dependencies

Modules that depend on each other create dependency graphs that the AI needs to load entirely. Break cycles by introducing abstractions or reorganizing responsibilities.

# Part V: Practical Application

## Review Checklist

When creating or reviewing code, ask:

### Locality

- Can the AI understand this feature by reading 3-4 files?
- Does the file fit comfortably in the context window (< 400 lines)?
- Is critical information at the beginning or end of the file?

### Separation

- Is business logic separated from IO?
- Are domain functions pure (same input → same output)?
- Are Commands and Queries separated?

### Expressiveness

- Do the types communicate intent without needing comments?
- Does the directory structure reveal the business domain?
- Do file and function names use domain language?

### Documentation

- Are non-obvious decisions documented with inline ADRs?
- Do the tests describe behavior as specification?
- Is the module's purpose clear in the header?

### Contracts

- Do module boundaries have explicit interfaces?
- Do dependencies point toward abstraction/stability?
- Is it possible to swap implementations without affecting contracts?

## Health Metrics

| Category      | Metric                  | Target  | Tool                      |
| ------------- | ----------------------- | ------- | ------------------------- |
| Complexity    | Cyclomatic per function | < 10    | clippy, complexity-report |
| Complexity    | Nesting depth           | < 4     | clippy                    |
| Size          | Lines per file          | 200-400 | tokei, cloc               |
| Size          | Lines per function      | < 50    | clippy                    |
| Coupling      | Dependencies per module | < 7     | cargo-modules             |
| Coupling      | Fan-out (imports)       | < 10    | manual analysis           |
| Coverage      | Domain tests            | > 80%   | tarpaulin, coverage       |
| Documentation | ADRs per decision       | 100%    | manual analysis           |

## Development Workflow

### 1. Before Starting

- Identify the affected feature/vertical slice
- Review existing related ADRs
- Check if there are tests specifying expected behavior

### 2. During Development

- Keep domain functions pure in the core
- Use specific types to communicate intent
- Document non-obvious decisions inline
- Write tests as specification before or alongside implementation

### 3. Before Committing

- Check complexity metrics
- Confirm that files are within the size limit
- Review whether necessary ADRs have been added
- Run tests to confirm specifications

### 4. Code Review

- Use the review checklist
- Verify that the AI could understand the change in isolation
- Confirm that contracts at boundaries are preserved

# Part VI: References

## Foundational Books

- "A Philosophy of Software Design" — John Ousterhout (deep modules, complexity)
- "Clean Architecture" — Robert C. Martin (dependency rule, screaming architecture)
- "Domain-Driven Design" — Eric Evans (ubiquitous language, bounded contexts)
- "Living Documentation" — Cyrille Martraire (documentation as code)

## Academic Papers

- "On the Criteria To Be Used in Decomposing Systems into Modules" — David Parnas, 1972
- "Learning a Metric for Code Readability" — Buse & Weimer, IEEE TSE 2010
- "Cognitive Dimensions of Notations" — Green & Petre
- "No Silver Bullet" — Fred Brooks, 1986
- "LongCodeBench: Evaluating Coding LLMs at 1M Context Windows" — arXiv 2025

## Talks and Screencasts

- "Boundaries" — Gary Bernhardt (Functional Core, Imperative Shell)
- "Functional architecture - The pits of success" — Mark Seemann
- "Effective Context Engineering for AI Agents" — Anthropic Engineering, 2025

## Architectural Patterns

- Vertical Slice Architecture — Jimmy Bogard
- Hexagonal Architecture / Ports and Adapters — Alistair Cockburn
- Architecture Decision Records — Michael Nygard
- C4 Model — Simon Brown

_— ExMA v1.0 —_

_Explicit Modular Architecture_

_For the era of human-AI collaboration_
