import frappe
from collections import defaultdict

@frappe.whitelist()
def get_employee_assessment_summary(employee, cycle_name):
    if not employee or not cycle_name:
        frappe.throw("Employee and Cycle Name are required")

    cycle = frappe.get_doc("Assessment Cycle", cycle_name)

    assignments = frappe.get_all(
        "Assessment Assignment",
        filters={
        "employee": employee,
        "assessment_cycle": cycle_name
        },
        pluck="name"
    )

    if not assignments:
        return {
            "employee": employee,
            "assessment_cycle": cycle_name,
            "total_assessments": 0,
            "completed_assessments": 0,
            "pending_assessments": 0,
            "completion_rate": 0
        }

    submissions = frappe.get_all(
        "Assessment Submission",
        filters={"assignment": ["in", assignments]},
        fields=["status"]
    )

    total = len(submissions)
    completed = sum(
        1 for s in submissions if s.status in ("Completed", "Submitted")
    )
    pending = total - completed
    completion_rate = round((completed / total) * 100, 2) if total else 0

    return {
        "employee": employee,
        "assessment_cycle": cycle_name,
        "total_assessments": total,
        "completed_assessments": completed,
        "pending_assessments": pending,
        "completion_rate": completion_rate
    }

# @frappe.whitelist()
# def get_employee_weighted_assessment_summary(employee, cycle_name,submission_id=None):
#     if not employee or not cycle_name:
#         frappe.throw("Employee and Cycle Name are required")

#     MAX_RATING = 5  # 🔑 important

#     cycle = frappe.get_doc("Assessment Cycle", cycle_name)

#     # --------------------------------------------------
#     # 1. Assignments for this employee in this cycle
#     # --------------------------------------------------
#     assignments = frappe.get_all(
#         "Assessment Assignment",
#         filters={
#         "employee": employee,
#         "assessment_cycle": cycle.name
#         },
#         pluck="name"
#     )

#     if not assignments:
#         return {}

#     # --------------------------------------------------
#     # 2. Latest submission per questionnaire (cycle-bound)
#     # --------------------------------------------------
#     submissions = frappe.get_all(
#         "Assessment Submission",
#         filters={
#             "assignment": ["in", assignments],
#             "status": ["in", ["Submitted", "Completed"]]
#         },
#         fields=["name", "questionnaire", "modified"],
#         order_by="modified desc"
#     )
    
#     if not submissions:
#         return {}

#     latest_by_questionnaire = {}
#     for s in submissions:
#         if s.questionnaire not in latest_by_questionnaire:
#             latest_by_questionnaire[s.questionnaire] = s.name

#     # --------------------------------------------------
#     # 3. Score containers
#     # --------------------------------------------------
#     stage_scores = defaultdict(float)
#     stage_max_scores = defaultdict(float)

#     substage_scores = defaultdict(lambda: defaultdict(float))
#     substage_max_scores = defaultdict(lambda: defaultdict(float))

#     # --------------------------------------------------
#     # 4. Process submissions
#     # --------------------------------------------------
#     for submission_name in latest_by_questionnaire.values():
#         submission = frappe.get_doc("Assessment Submission", submission_name)

#         # Weight matrix
#         weight_rows = frappe.get_all(
#             "Assessment Weight Matrix",
#             filters={"questionnaire": submission.questionnaire},
#             fields=[
#                 "question_text",
#                 "honeymoon_weight",
#                 "self_introspection_weight",
#                 "soul_searching_weight",
#                 "steady_state_weight"
#             ]
#         )
#         weight_map = {w.question_text: w for w in weight_rows}

#         questionnaire = frappe.get_doc("Questionnaire", submission.questionnaire)
#         question_meta = {
#             q.question_text: q for q in questionnaire.questions
#         }

#         for ans in submission.answers:
#             if not ans.rating:
#                 continue

#             q_text = ans.question
#             rating = float(ans.rating)

#             if q_text not in weight_map or q_text not in question_meta:
#                 continue

#             weights = weight_map[q_text]
#             meta = question_meta[q_text]

#             stage_weight_map = {
#                 "Honeymoon": weights.honeymoon_weight or 0,
#                 "Self-Introspection": weights.self_introspection_weight or 0,
#                 "Soul-Searching": weights.soul_searching_weight or 0,
#                 "Steady-State": weights.steady_state_weight or 0
#             }

#             contributions = {
#                 stage: rating * weight
#                 for stage, weight in stage_weight_map.items()
#             }

#             # 🔑 Dominant stage for this question
#             stage = max(contributions, key=contributions.get)
#             weight = stage_weight_map[stage]

#             score = rating * weight
#             max_score = MAX_RATING * weight

#             substage = meta.sub_stage or "Unknown"

#             # Accumulate
#             stage_scores[stage] += score
#             stage_max_scores[stage] += max_score

#             substage_scores[stage][substage] += score
#             substage_max_scores[stage][substage] += max_score

#     if not stage_scores:
#         return {}

#     # --------------------------------------------------
#     # 5. Percentages
#     # --------------------------------------------------
#     stage_percentages = {
#         stage: round((stage_scores[stage] / stage_max_scores[stage]) * 100, 2)
#         if stage_max_scores[stage] else 0
#         for stage in stage_scores
#     }

#     substage_percentages = {
#         stage: {
#             sub: round(
#                 (substage_scores[stage][sub] / substage_max_scores[stage][sub]) * 100,
#                 2
#             ) if substage_max_scores[stage][sub] else 0
#             for sub in substage_scores[stage]
#         }
#         for stage in substage_scores
#     }

#     # --------------------------------------------------
#     # 6. Dominant stage & substage
#     # --------------------------------------------------
#     dominant_stage = max(stage_scores, key=stage_scores.get)

#     dominant_substage = (
#         max(
#             substage_scores[dominant_stage],
#             key=substage_scores[dominant_stage].get
#         )
#         if substage_scores.get(dominant_stage)
#         else None
#     )

#     # --------------------------------------------------
#     # 7. Final response
#     # --------------------------------------------------
#     return {
#         "employee": employee,
#         "assessment_cycle": cycle_name,
#         "questionnaires_considered": list(latest_by_questionnaire.keys()),
#         "dominant_stage": dominant_stage,
#         "dominant_sub_stage": dominant_substage,
#         "stages": [
#             {
#                 "stage": stage,
#                 "score": round(stage_scores[stage], 2),
#                 "percentage": stage_percentages.get(stage, 0),
#                 "sub_stages": [
#                     {
#                         "sub_stage": sub,
#                         "score": round(substage_scores[stage][sub], 2),
#                         "percentage": substage_percentages[stage].get(sub, 0)
#                     }
#                     for sub in substage_scores[stage]
#                 ]
#             }
#             for stage in stage_scores
#         ]
#     }

@frappe.whitelist()
def get_employee_weighted_assessment_summary(employee, cycle_name, submission_id=None):
    if not employee or not cycle_name:
        frappe.throw("Employee and Cycle Name are required")

    MAX_RATING = 5  # 🔑 important

    cycle = frappe.get_doc("Assessment Cycle", cycle_name)

    # --------------------------------------------------
    # 1. Assignments for this employee in this cycle
    # --------------------------------------------------
    assignments = frappe.get_all(
        "Assessment Assignment",
        filters={
            "employee": employee,
            "assessment_cycle": cycle.name
        },
        pluck="name"
    )

    if not assignments:
        return {}

    # --------------------------------------------------
    # 2. Resolve submissions (cycle OR single submission)
    # --------------------------------------------------
    latest_by_questionnaire = {}

    if submission_id:
        # 🔹 SINGLE SUBMISSION MODE
        submission = frappe.get_doc("Assessment Submission", submission_id)

        # Safety validations
        if submission.employee != employee:
            frappe.throw("Submission does not belong to this employee")

        if submission.assessment_cycle != cycle_name:
            frappe.throw("Submission does not belong to this assessment cycle")

        if submission.status not in ("Submitted", "Completed"):
            frappe.throw("Submission is not completed")

        latest_by_questionnaire[submission.questionnaire] = submission.name

    else:
        # 🔹 CYCLE MODE (latest submission per questionnaire)
        submissions = frappe.get_all(
            "Assessment Submission",
            filters={
                "assignment": ["in", assignments],
                "status": ["in", ["Submitted", "Completed"]]
            },
            fields=["name", "questionnaire", "modified"],
            order_by="modified desc"
        )

        if not submissions:
            return {}

        for s in submissions:
            if s.questionnaire not in latest_by_questionnaire:
                latest_by_questionnaire[s.questionnaire] = s.name

    # --------------------------------------------------
    # 3. Score containers
    # --------------------------------------------------
    stage_scores = defaultdict(float)
    stage_max_scores = defaultdict(float)

    substage_scores = defaultdict(lambda: defaultdict(float))
    substage_max_scores = defaultdict(lambda: defaultdict(float))

    # --------------------------------------------------
    # 4. Process submissions
    # --------------------------------------------------
    for submission_name in latest_by_questionnaire.values():
        submission = frappe.get_doc("Assessment Submission", submission_name)

        # Weight Matrix
        weight_rows = frappe.get_all(
            "Assessment Weight Matrix",
            filters={"questionnaire": submission.questionnaire},
            fields=[
                "question_text",
                "honeymoon_weight",
                "self_introspection_weight",
                "soul_searching_weight",
                "steady_state_weight"
            ]
        )
        weight_map = {w.question_text: w for w in weight_rows}

        # Questionnaire metadata
        questionnaire = frappe.get_doc("Questionnaire", submission.questionnaire)
        question_meta = {
            q.question_text: q for q in questionnaire.questions
        }

        for ans in submission.answers:
            if not ans.rating:
                continue

            q_text = ans.question
            rating = float(ans.rating)

            if q_text not in weight_map or q_text not in question_meta:
                continue

            weights = weight_map[q_text]
            meta = question_meta[q_text]

            stage_weight_map = {
                "Honeymoon": weights.honeymoon_weight or 0,
                "Self-Introspection": weights.self_introspection_weight or 0,
                "Soul-Searching": weights.soul_searching_weight or 0,
                "Steady-State": weights.steady_state_weight or 0
            }

            contributions = {
                stage: rating * weight
                for stage, weight in stage_weight_map.items()
            }

            # 🔑 Dominant stage for this question
            stage = max(contributions, key=contributions.get)
            weight = stage_weight_map[stage]

            score = rating * weight
            max_score = MAX_RATING * weight

            substage = meta.sub_stage or "Unknown"

            # Accumulate
            stage_scores[stage] += score
            stage_max_scores[stage] += max_score

            substage_scores[stage][substage] += score
            substage_max_scores[stage][substage] += max_score

    if not stage_scores:
        return {}

    # --------------------------------------------------
    # 5. Percentages
    # --------------------------------------------------
    stage_percentages = {
        stage: round((stage_scores[stage] / stage_max_scores[stage]) * 100, 2)
        if stage_max_scores[stage] else 0
        for stage in stage_scores
    }

    substage_percentages = {
        stage: {
            sub: round(
                (substage_scores[stage][sub] / substage_max_scores[stage][sub]) * 100,
                2
            ) if substage_max_scores[stage][sub] else 0
            for sub in substage_scores[stage]
        }
        for stage in substage_scores
    }

    # --------------------------------------------------
    # 6. Dominant stage & substage
    # --------------------------------------------------
    dominant_stage = max(stage_scores, key=stage_scores.get)

    dominant_substage = (
        max(
            substage_scores[dominant_stage],
            key=substage_scores[dominant_stage].get
        )
        if substage_scores.get(dominant_stage)
        else None
    )

    # --------------------------------------------------
    # 7. Final response (UNCHANGED FORMAT)
    # --------------------------------------------------
    return {
        "employee": employee,
        "assessment_cycle": cycle_name,
        "questionnaires_considered": list(latest_by_questionnaire.keys()),
        "dominant_stage": dominant_stage,
        "dominant_sub_stage": dominant_substage,
        "stages": [
            {
                "stage": stage,
                "score": round(stage_scores[stage], 2),
                "percentage": stage_percentages.get(stage, 0),
                "sub_stages": [
                    {
                        "sub_stage": sub,
                        "score": round(substage_scores[stage][sub], 2),
                        "percentage": substage_percentages[stage].get(sub, 0)
                    }
                    for sub in substage_scores[stage]
                ]
            }
            for stage in stage_scores
        ]
    }
    
@frappe.whitelist()
def get_employee_cycle_transition_lab(employee, cycle_name):
    if not employee or not cycle_name:
        frappe.throw("Employee and Cycle Name are required")

    cycle = frappe.get_doc("Assessment Cycle", cycle_name)

    assignments = frappe.get_all(
        "Assessment Assignment",
        filters={
        "employee": employee,
        "assessment_cycle": cycle_name
        },
        pluck="name"
    )

    if not assignments:
        return {}

    submissions = frappe.get_all(
        "Assessment Submission",
        filters={
            "assignment": ["in", assignments],
            "status": ["in", ["Submitted", "Completed"]]
        },
        fields=["name", "questionnaire", "modified"],
        order_by="modified desc"
    )

    if not submissions:
        return {}

    last_submitted_on = submissions[0].modified

    latest_by_questionnaire = {}
    for s in submissions:
        if s.questionnaire not in latest_by_questionnaire:
            latest_by_questionnaire[s.questionnaire] = s.name

    stage_scores = defaultdict(float)

    for submission_name in latest_by_questionnaire.values():
        submission = frappe.get_doc("Assessment Submission", submission_name)

        weight_rows = frappe.get_all(
            "Assessment Weight Matrix",
            filters={"questionnaire": submission.questionnaire},
            fields=[
                "question_text",
                "honeymoon_weight",
                "self_introspection_weight",
                "soul_searching_weight",
                "steady_state_weight"
            ]
        )
        weight_map = {w.question_text: w for w in weight_rows}

        for ans in submission.answers:
            if not ans.rating:
                continue

            q_text = ans.question
            rating = float(ans.rating)
            if q_text not in weight_map:
                continue

            weights = weight_map[q_text]
            contributions = {
                "Honeymoon": rating * (weights.honeymoon_weight or 0),
                "Self-Introspection": rating * (weights.self_introspection_weight or 0),
                "Soul-Searching": rating * (weights.soul_searching_weight or 0),
                "Steady-State": rating * (weights.steady_state_weight or 0)
            }

            stage = max(contributions, key=contributions.get)
            stage_scores[stage] += contributions[stage]

    total_score = sum(stage_scores.values())
    dominant_stage = max(stage_scores, key=stage_scores.get)

    return {
        "employee": employee,
        "assessment_cycle": cycle_name,
        "status": cycle.status,
        "last_submitted_on": last_submitted_on,
        "stages": [
            {
                "stage": stage,
                "score": round(score, 2),
                "percentage": round((score / total_score) * 100, 2)
            }
            for stage, score in stage_scores.items()
        ],
        "dominant_stage": dominant_stage
    }
import frappe
 
@frappe.whitelist()
def get_employee_cycles_with_questionnaires(employee, status=None):
    if not employee:
        frappe.throw("Employee is required")

    # --------------------------------------------------
    # 1. Normalize status filter (optional)
    # --------------------------------------------------
    status_filter = None
    if status:
        if isinstance(status, str):
            # Accept "Active,Completed"
            status_filter = [s.strip() for s in status.split(",") if s.strip()]
        elif isinstance(status, list):
            status_filter = status

    # --------------------------------------------------
    # 2. Cycles assigned to employee
    # --------------------------------------------------
    cycle_links = frappe.get_all(
        "Assessment Cycle Employee",
        filters={
            "employee": employee,
            "include": 1,
            "parenttype": "Assessment Cycle"
        },
        fields=["parent"]
    )

    cycle_names = list({c.parent for c in cycle_links})
    if not cycle_names:
        return []

    # --------------------------------------------------
    # 3. Build cycle filters
    # --------------------------------------------------
    cycle_filters = {
        "name": ["in", cycle_names]
    }

    if status_filter:
        cycle_filters["status"] = ["in", status_filter]

    cycles = frappe.get_all(
        "Assessment Cycle",
        filters=cycle_filters,
        fields=[
            "name",
            "cycle_name",
            "status",
            "dimension",
            "start_date",
            "end_date"
        ],
        order_by="start_date desc"
    )

    response = []

    # --------------------------------------------------
    # 4. Resolve questionnaires via Dimension
    # --------------------------------------------------
    for cycle in cycles:
        dimension = frappe.get_doc("Assessment Dimension", cycle.dimension)

        questionnaires = []

        if dimension.allow_self:
            questionnaires.append({"name": "SELF"})
        if dimension.allow_boss:
            questionnaires.append({"name": "BOSS"})
        if dimension.allow_department:
            questionnaires.append({"name": "DEPT"})
        if dimension.allow_company:
            questionnaires.append({"name": "COMPANY"})

        response.append({
            "assessment_cycle": cycle.name,
            "cycle_name": cycle.cycle_name,
            "status": cycle.status,
            "dimension": cycle.dimension,
            "start_date": cycle.start_date,
            "end_date": cycle.end_date,
            "questionnaires": questionnaires
        })

    return response

@frappe.whitelist()
def get_employee_cycle_detailed_dashboard(employee, status=None):
    if not employee:
        frappe.throw("Employee is required")

    # --------------------------------------------------
    # Normalize optional status filter
    # --------------------------------------------------
    status_filter = None
    if status:
        if isinstance(status, str):
            status_filter = [s.strip() for s in status.split(",") if s.strip()]
        elif isinstance(status, list):
            status_filter = status

    # --------------------------------------------------
    # 1. Cycles assigned to employee
    # --------------------------------------------------
    cycle_links = frappe.get_all(
        "Assessment Cycle Employee",
        filters={
            "employee": employee,
            "include": 1,
            "parenttype": "Assessment Cycle"
        },
        pluck="parent"
    )

    if not cycle_links:
        return []

    cycle_filters = {"name": ["in", cycle_links]}
    if status_filter:
        cycle_filters["status"] = ["in", status_filter]

    cycles = frappe.get_all(
        "Assessment Cycle",
        filters=cycle_filters,
        fields=[
            "name",
            "cycle_name",
            "status",
            "dimension",
            "start_date",
            "end_date"
        ],
        order_by="start_date desc"
    )

    response = []

    # --------------------------------------------------
    # 2. Process each cycle
    # --------------------------------------------------
    for cycle in cycles:

        # ----------------------------------------------
        # Assignments for this cycle + employee
        # ----------------------------------------------
        assignments = frappe.get_all(
            "Assessment Assignment",
            filters={
                "employee": employee,
                "assessment_cycle": cycle.name
            },
            pluck="name"
        )

        if not assignments:
            continue

        # ----------------------------------------------
        # Submissions under this cycle
        # ----------------------------------------------
        submissions = frappe.get_all(
            "Assessment Submission",
            filters={"assignment": ["in", assignments]},
            fields=[
                "name",
                "questionnaire",
                "status",
                "modified"
            ],
            order_by="modified desc"
        )

        if not submissions:
            continue

        cycle_last_submitted = submissions[0].modified

        cycle_stage_scores = defaultdict(float)
        cycle_stage_max = defaultdict(float)

        questionnaire_items = []

        # ----------------------------------------------
        # 3. Per-submission (inner items)
        # ----------------------------------------------
        for sub in submissions:
            submission = frappe.get_doc("Assessment Submission", sub.name)

            weight_rows = frappe.get_all(
                "Assessment Weight Matrix",
                filters={"questionnaire": submission.questionnaire},
                fields=[
                    "question_text",
                    "honeymoon_weight",
                    "self_introspection_weight",
                    "soul_searching_weight",
                    "steady_state_weight"
                ]
            )
            weight_map = {w.question_text: w for w in weight_rows}

            questionnaire = frappe.get_doc("Questionnaire", submission.questionnaire)
            question_meta = {
                q.question_text: q
                for q in questionnaire.questions
            }

            stage_scores = defaultdict(float)
            stage_max = defaultdict(float)
            substage_scores = defaultdict(lambda: defaultdict(float))

            for ans in submission.answers:
                if not ans.rating:
                    continue

                q_text = ans.question
                rating = float(ans.rating)

                if q_text not in weight_map or q_text not in question_meta:
                    continue

                weights = weight_map[q_text]
                meta = question_meta[q_text]

                contributions = {
                    "Honeymoon": rating * (weights.honeymoon_weight or 0),
                    "Self-Introspection": rating * (weights.self_introspection_weight or 0),
                    "Soul-Searching": rating * (weights.soul_searching_weight or 0),
                    "Steady-State": rating * (weights.steady_state_weight or 0)
                }

                stage = max(contributions, key=contributions.get)
                score = contributions[stage]
                substage = meta.sub_stage or "Unknown"

                stage_scores[stage] += score
                substage_scores[stage][substage] += score
                stage_max[stage] += rating

                cycle_stage_scores[stage] += score
                cycle_stage_max[stage] += rating

            dominant_stage = max(stage_scores, key=stage_scores.get) if stage_scores else None
            dominant_substage = (
                max(substage_scores[dominant_stage], key=substage_scores[dominant_stage].get)
                if dominant_stage and substage_scores[dominant_stage]
                else None
            )

            stage_percentages = {
                s: round((stage_scores[s] / stage_max[s]) * 100, 2)
                if stage_max[s] else 0
                for s in stage_scores
            }

            questionnaire_items.append({
                "submission_id": submission.name,
                "questionnaire": submission.questionnaire,
                "status": submission.status,
                "last_submitted_on": submission.modified,
                "dominant_stage": dominant_stage,
                "dominant_sub_stage": dominant_substage,
                "stages": [
                    {
                        "stage": s,
                        "percentage": stage_percentages.get(s, 0)
                    }
                    for s in stage_percentages
                ]
            })

        # ----------------------------------------------
        # 4. Cycle-level aggregation
        # ----------------------------------------------
        cycle_percentages = {
            s: round((cycle_stage_scores[s] / cycle_stage_max[s]) * 100, 2)
            if cycle_stage_max[s] else 0
            for s in cycle_stage_scores
        }

        cycle_dominant_stage = (
            max(cycle_stage_scores, key=cycle_stage_scores.get)
            if cycle_stage_scores
            else None
        )
        cycle_dominant_substage = None

        response.append({
            "assessment_cycle": cycle.name,
            "cycle_name":cycle.cycle_name,
            "status": cycle.status,
            "start_date": cycle.start_date,
            "end_date": cycle.end_date,
            "last_submitted_on": cycle_last_submitted,
            "dominant_stage": cycle_dominant_stage,
            "stages": [
                {
                    "stage": s,
                    "percentage": cycle_percentages.get(s, 0)
                }
                for s in cycle_percentages
            ],
            "items": questionnaire_items
        })

    return response