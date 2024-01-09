import pytest
from bloom_lims.bdb import BLOOMdb3
from bloom_lims.bdb import BloomWorkflow, BloomWorkflowStep
import sys


def test_create_accessioning_wf():
    bob_wf = BloomWorkflow(BLOOMdb3())

    bob_wfs = BloomWorkflowStep(BLOOMdb3())

    # make a control
    rgnt = bob_wfs.create_instance_by_template_components(
        "content", "control", "giab-HG002", "1.0", "active"
    )[0][0]
    # put in a tube
    tube = bob_wfs.create_instance_by_template_components(
        "container", "tube", "tube-generic-10ml", "1.0", "active"
    )[0][0]
    # put the reagent in the tube
    bob_wfs.session.flush()
    bob_wfs.session.commit()
    bob_wfs.create_generic_instance_lineage_by_euids(tube.euid, rgnt.euid)



    # make a control
    rgnt2 = bob_wfs.create_instance_by_template_components(
        "content", "control", "water-ntc", "1.0", "active"
    )[0][0]
    
    
    # put in a tube
    tube2 = bob_wfs.create_instance_by_template_components(
        "container", "tube", "tube-generic-10ml", "1.0", "active"
    )[0][0]
    # put the reagent in the tube
    bob_wfs.session.flush()
    bob_wfs.session.commit()
    bob_wfs.create_generic_instance_lineage_by_euids(tube.euid, rgnt.euid)

    records = (
        bob_wfs.session.query(bob_wfs.Base.classes.workflow_template)
        .filter(
            bob_wfs.Base.classes.workflow_template.b_sub_type
            == "accession-package-kit-tubes-testreq"
        )
        .all()
    )

    wf = bob_wfs.create_instances(records[0].euid)[0][0]

    action_group = "accessioning"
    action = "action/workflow/create_package_and_first_workflow_step/1.0"
    action_data = wf.json_addl["action_groups"][action_group]["actions"][action]
    # action_data = wf.json_addl["actions"]["create_package_and_first_workflow_step"]
    action_data["captured_data"][
        "Tracking Number"
    ] = "1001897582860000245100773464327825"
    action_data["captured_data"]["Fedex Tracking Data"] = {}

    bob_wf.do_action(wf.euid, action, action_group, action_data)

    # wfs = bob_wf.do_action_create_package_and_first_workflow_step(wf.euid, action_data)
    wfs = wf.parent_of_lineages[0].child_instance
    assert hasattr(wfs, "euid") == True

    b_action_group = "create_child"  # change to child
    b_action = "action/workflow_step_accessioning/create_child_container_and_link_child_workflow_step/1.0"
    wfs_action_data = wfs.json_addl["action_groups"][b_action_group]["actions"][
        b_action
    ]

    bob_wfs.do_action(wfs.euid, b_action, b_action_group, wfs_action_data)
    # child_cont = bob_wfs.do_action_create_child_container_and_link_child_workflow_step(
    #    wfs.euid, wfs_action_data
    # )

    child_wfs = ""
    for i in wfs.parent_of_lineages:
        if i.child_instance.btype == "accessioning-steps":
            child_wfs = i.child_instance

    assert hasattr(child_wfs, "euid") == True
    c_action_group = "specimen_actions"  # change to child
    c_action = "action/workflow_step_accessioning/create_child_container_and_link_child_workflow_step_specimen/1.0"
    c_wfs_action_data = child_wfs.json_addl["action_groups"][c_action_group]["actions"][
        c_action
    ]

    # new_child_cont = bob_wfs.do_action_create_child_container_and_link_child_workflow_step(
    #    child_cont.euid, wfs2_action_data
    # )
    bob_wfs.do_action(child_wfs.euid, c_action, c_action_group, c_wfs_action_data)

    new_child_wfs = ""
    for i in child_wfs.parent_of_lineages:
        if i.child_instance.super_type == "workflow_step":
            new_child_wfs = i.child_instance
    assert hasattr(new_child_wfs, "euid") == True

    trf_wfs = bob_wfs.do_action(
        new_child_wfs.euid,
        "action/workflow_step_accessioning/create_test_req_and_link_child_workflow_step_dup/1.0",
        "test_req",
        new_child_wfs.json_addl["action_groups"]["test_req"]["actions"][
            "action/workflow_step_accessioning/create_test_req_and_link_child_workflow_step_dup/1.0"
        ],
    )

    assert hasattr(trf_wfs, "euid") == True

    trf_child_wfs = ""
    trf_child_cont = ""
    for i in trf_wfs.parent_of_lineages:
        if i.child_instance.super_type == "workflow_step":
            trf_child_wfs = i.child_instance
        if i.child_instance.super_type == "container":
            trf_child_cont = i.child_instance

    trf = ""
    for i in trf_child_cont.child_of_lineages:
        if i.parent_instance.super_type == "test_requisition":
            trf = i.parent_instance

    trf_assay_data = trf.json_addl["action_groups"]["test_requisitions"]["actions"][
        "action/test_requisitions/add_container_to_assay_q/1.0"
    ]
    # This is super brittle, how I am currently linking Assay to TestReq...
    # = tr.json_addl["actions"]["add_container_to_assay_q"]
    trf_assay_data["captured_data"]["assay_selection"] = "hla-typing/1.2"
    trf_assay_data["captured_data"]["Container EUID"] = trf_child_cont.euid

    wfs_queue = bob_wfs.do_action(
        trf.euid,
        action_group="test_requisitions",
        action="action/test_requisitions/add_container_to_assay_q/1.0",
        action_ds=trf_assay_data,
    )
    assert hasattr(wfs_queue, "euid") == True

    scanned_bcs = trf_child_cont.euid
    # for i in wfs_queue.parent_of_lineages:
    #    if i.child_instance.btype == "tube":
    #        scanned_bcs += f"\n{i.child_instance.euid}"

    q_wfs = ""
    for i in trf_child_cont.child_of_lineages:
        if i.parent_instance.b_sub_type == "plasma-isolation-queue-available":
            q_wfs = i.parent_instance

    piso_q_action_data = q_wfs.json_addl["action_groups"]["tube_xfer"]["actions"][
        "action/workflow_step_queue/link_tubes_auto/1.0"
    ]
    piso_q_action_data["captured_data"]["discard_barcodes"] = scanned_bcs
    wfs_plasma = bob_wfs.do_action(
        q_wfs.euid,
        action_group="tube_xfer",
        action="action/workflow_step_queue/link_tubes_auto/1.0",
        action_ds=piso_q_action_data,
    )  # _link_tubes_auto(wfs_queue.euid, piso_q_action_data)

    plasma_cont = None
    for i in trf_child_cont.parent_of_lineages:
        if i.child_instance.b_sub_type == "tube-generic-10ml":
            plasma_cont = i.child_instance
    scanned_bcs_plasma = plasma_cont.euid

    for i in plasma_cont.child_of_lineages:
        if i.parent_instance.super_type == "workflow_step":
            pi_wfs = i.parent_instance

    action_ds_plasma = pi_wfs.json_addl["action_groups"]["fill_plate"]["actions"][
        "action/workflow_step_queue/fill_plate_undirected/1.0"
    ]
    # wfs_plasma.json_addl["actions"]["fill_plate_undirected"]
    action_ds_plasma["captured_data"]["discard_barcodes"] = scanned_bcs_plasma
    wfs_plt = bob_wfs.do_action(
        pi_wfs.euid,
        action_group="fill_plate",
        action="action/workflow_step_queue/fill_plate_undirected/1.0",
        action_ds=action_ds_plasma,
    )
    # _fill_plate_undirected(wfs_plasma.euid, action_ds_plasma)

    ### ENDING WITH ANEXTRACTION PLATE!  Need to check quant and add more from here.

    plt_fill_wfs = ""
    for i in wfs_plt.parent_of_lineages:
        if i.child_instance.super_type == "workflow_step":
            plt_fill_wfs = i.child_instance

    action_data_dat = plt_fill_wfs.json_addl["action_groups"]["plate_operations"][
        "actions"
    ]["action/workflow_step_plate_operations/cfdna_quant/1.0"]
    # action_data_dat = wfs_plt.json_addl["actions"]["cfdna_quant"]
    action_data_dat["captured_data"]["gdna_quant"] = ""
    bob_wfs.do_action(
        plt_fill_wfs.euid,
        action_group="plate_operations",
        action="action/workflow_step_plate_operations/cfdna_quant/1.0",
        action_ds=action_data_dat,
    )

    eplt = None
    for i in plt_fill_wfs.parent_of_lineages:
        if i.child_instance.super_type == "workflow_step":
            eplt = i.child_instance

    next_plate = ""
    for i in plt_fill_wfs.parent_of_lineages:
        if i.child_instance.btype == "plate":
            next_plate = i.child_instance.euid

    stamp_action_data = plt_fill_wfs.json_addl["action_groups"]["plate_operations"][
        "actions"
    ]["action/workflow_step_plate_operations/stamp_copy_plate/1.0"]
    stamp_action_data["captured_data"]["plate_euid"] = next_plate
    bob_wfs.do_action(
        plt_fill_wfs.euid,
        action_group="plate_operations",
        action="action/workflow_step_plate_operations/stamp_copy_plate/1.0",
        action_ds=stamp_action_data,
    )

    ## Third copy of a plate
    for i in plt_fill_wfs.parent_of_lineages:
        if i.child_instance.btype == "plate-operations":
            sec_stamp_wfs = i.child_instance

    for i in sec_stamp_wfs.parent_of_lineages:
        if i.child_instance.super_type == "container":
            next_plate2 = i.child_instance.euid

    stamp_action_data2 = sec_stamp_wfs.json_addl["action_groups"]["plate_operations"][
        "actions"
    ]["action/workflow_step_plate_operations/stamp_copy_plate/1.0"]
    stamp_action_data2["captured_data"]["plate_euid"] = next_plate2
    stamp_wfs2 = bob_wfs.do_action(
        sec_stamp_wfs.euid,
        action_group="plate_operations",
        action="action/workflow_step_plate_operations/stamp_copy_plate/1.0",
        action_ds=stamp_action_data2,
    )

    # make a control
    rgnt = bob_wfs.create_instance_by_template_components(
        "content", "control", "giab-HG002", "1.0", "active"
    )[0][0]
    # put in a tube
    tube = bob_wfs.create_instance_by_template_components(
        "container", "tube", "tube-generic-10ml", "1.0", "active"
    )[0][0]
    # put the reagent in the tube

    bob_wfs.create_generic_instance_lineage_by_euids(tube.euid, rgnt.euid)
