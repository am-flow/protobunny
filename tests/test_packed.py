from unittest.mock import ANY

import betterproto
import numpy as np
import numpy.testing as npt

from . import tests


def test_complex_message() -> None:
    data = {
        "distance_cdf": np.array(
            [0.0, 0.1761976826283381, 0.3360906092222883, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        ),
        "distance_cdf_step": np.float_(0.05),
        "part_bbox_extent": np.array([198.5670928955078, 201.40457153320312, 152.95013427734375]),
        "part_bbox_center": np.array([6.441764831542969, -19.629409790039062, 76.47506713867188]),
        "part_bbox_rotation": np.array(
            [
                [0.9950731992721558, -0.09914305806159973, 0.0],
                [0.09914308041334152, 0.9950731992721558, -0.0],
                [0.0, 0.0, 1.0],
            ]
        ),
        "scan_bbox_extent": np.array([198.28387451171875, 200.86383056640625, 153.09780883789062]),
        "scan_bbox_center": np.array([6.441764831542969, -19.629409790039062, 76.54890441894531]),
        "scan_bbox_rotation": np.array(
            [
                [0.9950731992721558, -0.09914305806159973, 0.0],
                [0.09914308041334152, 0.9950731992721558, -0.0],
                [0.0, 0.0, 1.0],
            ]
        ),
        "max_missing_area_centroid": np.array(
            [
                82.57372283935547,
                -29.880002975463867,
                15.93416690826416,
            ]
        ),
        "max_missing_area_size": np.int_(572),
        "part_pose": np.array(
            [
                [
                    -0.6797157804915129,
                    -0.34871117468390145,
                    0.6452803650368037,
                    55.72332582210536,
                ],
                [
                    -0.7334754848378484,
                    0.3235935191340345,
                    -0.5977465618459061,
                    -73.10345511815211,
                ],
                [
                    -0.0003676277833084182,
                    -0.8795952145834851,
                    -0.4757227565212457,
                    -0.24029829148940943,
                ],
                [1.0741441069939128e-10, 5.733221675185973e-10, -5.05052055732591e-10, 1.0],
            ]
        ),
    }

    # Build the Evaluation message
    msg = tests.TestMessage(
        content="test",
        number=12,
        detail="ed1b0017-1c49-4279-b3f3-c23195a4ed33",
        # `options` is a pb.commons.JsonContent instance but it's serialized internally by the library
        options=data,
    )
    # Serialize the Evaluation message. This will also serialize dictionaries
    # for JsonContent fields in the message.
    # Internally the dict is converted to a JsonContent value
    serialized = bytes(msg)
    # Deserialize (it will also deserialize the JsonContent to a dictionary
    # Note: numpy arrays are not preserved and are serialized back as lists.
    deserialized = tests.TestMessage().parse(serialized)
    # We assert that deserialized.options == data
    # Since the original data has numpy arrays as values, we use np.assert_array_equal
    for k, v in deserialized.options.items():
        npt.assert_array_equal(v, data[k])
    # Test to_dict and to_pydict
    res = msg.to_dict(casing=betterproto.Casing.SNAKE)
    for k, v in res["options"].items():
        npt.assert_array_equal(v, data[k])
    assert msg.to_dict(casing=betterproto.Casing.SNAKE) == dict(
        content="test",
        number="12",
        detail="ed1b0017-1c49-4279-b3f3-c23195a4ed33",
        options=ANY,
    )
    logged = deserialized.to_pydict(casing=betterproto.Casing.SNAKE)
    for k, v in logged["options"].items():
        npt.assert_array_equal(v, data[k])


def test_int_floats() -> None:
    msg = tests.tasks.TaskMessage(
        content="test",
        bbox=[1, -1, 1, 100],
        weights=[123.0, 0.5, 100_000],
    )
    # serialize
    ser = bytes(msg)
    # deserialize
    deser = tests.tasks.TaskMessage().parse(ser)
    assert msg.bbox == [round(v, 2) for v in deser.bbox]
